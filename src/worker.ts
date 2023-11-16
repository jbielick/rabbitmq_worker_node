import { randomBytes } from "node:crypto";
import { EventEmitter } from "node:events";
import type { Message } from "amqplib";
import pLimit, { LimitFunction } from "p-limit";
import os from "node:os";
import { Exchange } from "./exchange";
import { Queue } from "./queue";
import { AmqpChannel } from ".";
import { Session, ConnectionFactoryFunction } from "./session";
import assert from "node:assert";

const SIX_MONTHS_IN_MS = 1.577e10;

export type ParserAlias = "json" | "none";
const JSON_PARSER: ParserFunction = (raw) => JSON.parse(raw.toString("utf8"));
const NOOP_PARSER: ParserFunction = (raw) => raw;

export interface ParserFunction {
  (buffer: Buffer): any;
}
export type Context = Pick<Message, "properties" | "fields"> | StringIndexed;
export interface PerformFunction {
  (data: any, ctx: Context): any;
}

export interface WorkerOptions {
  tag?: string;
  concurrency: number;
  retries: number;
  parser: ParserFunction;
  deadJobRetensionMs: number;
}
export interface PartialWorkerOptions extends Partial<WorkerOptions> {}
export type WorkerState = "idle" | "running" | "stopping";
export type ValidatedWorker =
  | Worker & {
      _perform: PerformFunction;
      _exchange: Exchange;
      _parser: ParserFunction;
      _queue: Queue;
      _routingKey: string;
      _bindArgs: StringIndexed;
      _retryQ: Queue;
      _requeueX: Exchange;
      _deadQ: Queue;
      createConnection: ConnectionFactoryFunction;
      options: WorkerOptions;
    };

export class Worker extends EventEmitter {
  protected static _defaults: WorkerOptions = {
    concurrency: process.env.CONCURRENCY ? Number(process.env.CONCURRENCY) : 10,
    retries: 25,
    parser: JSON_PARSER,
    deadJobRetensionMs: SIX_MONTHS_IN_MS,
  };
  protected static createConnection?: ConnectionFactoryFunction;

  stopping?: Promise<boolean>;
  protected _options: WorkerOptions;
  protected _state: WorkerState = "idle";
  protected _session?: Session;
  protected _createConnection?: ConnectionFactoryFunction;
  protected _perform?: PerformFunction;
  protected _exchange?: Exchange;
  protected _parser?: ParserFunction;
  protected _queue?: Queue;
  protected _routingKey?: string;
  protected _bindArgs?: StringIndexed;
  protected _retryQ?: Queue;
  protected _requeueX?: Exchange;
  protected _deadQ?: Queue;

  constructor(options: PartialWorkerOptions = {}) {
    super();
    this._options = {
      ...(this.constructor as typeof Worker).defaults,
      ...options,
    };
  }

  static set defaults(options: PartialWorkerOptions) {
    this._defaults = {
      ...this._defaults,
      ...options,
    };
  }

  static get defaults(): WorkerOptions {
    return this._defaults;
  }

  static set connect(connectionFactory: ConnectionFactoryFunction) {
    this.createConnection = connectionFactory;
  }

  toConnect(connectionFactory: ConnectionFactoryFunction) {
    this._createConnection = connectionFactory;
    return this;
  }

  from(exchange: Exchange | string) {
    if (!(exchange instanceof Exchange)) exchange = new Exchange(exchange);
    this._exchange = exchange;
    if (!this._queue) this.bind(exchange.name, "#");
    return this;
  }

  bind(queue: Queue | string, routingKey = "#", args: StringIndexed = {}) {
    if (!(queue instanceof Queue)) queue = new Queue(queue);
    this._queue = queue;
    this._routingKey = routingKey;
    this._bindArgs = args;
    this._requeueX = new Exchange(`${this._queue.name}.requeue`, {
      type: "topic",
    });
    this._retryQ = new Queue(`${this._queue.name}.retry`).with({
      deadLetterExchange: this._requeueX.name,
    });
    this._deadQ = new Queue(`${this._queue.name}.dead`);
    return this;
  }

  parse(contentType: ParserFunction | ParserAlias): this {
    let parser;
    if (typeof contentType === "function") {
      parser = contentType;
    } else if (contentType === "json") {
      parser = JSON_PARSER;
    } else if (contentType === "none") {
      parser = NOOP_PARSER;
    } else {
      throw new Error(`unknown content type argument: ${contentType}`);
    }
    this._parser = parser;
    return this;
  }

  protected get parser() {
    return this._parser || this.options.parser || NOOP_PARSER;
  }

  with(options: PartialWorkerOptions = {}) {
    this._options = {
      ...(this.constructor as typeof Worker).defaults,
      ...options,
    };
    return this;
  }

  perform(fn: PerformFunction) {
    this._perform = fn;
    return this;
  }

  get tag() {
    if (this.options.tag) return this.options.tag;
    this._options.tag = workerId();
    return this._options.tag;
  }

  get options(): WorkerOptions {
    return { ...this._options };
  }

  get createConnection() {
    return (
      this._createConnection ||
      (this.constructor as typeof Worker).createConnection
    );
  }

  get concurrency() {
    return this.options.concurrency;
  }

  protected parseMessage(buffer: Buffer) {
    return this.parser(buffer);
  }

  #validate(): asserts this is ValidatedWorker {
    assert(
      this.createConnection,
      "Cannot start without a connection factory. Please call .toConnect(...) and provide a connection factory function."
    );
    assert(
      this._queue,
      "Cannot start without a queue. Please call .from(exchange) or .bind(queue) to setup a queue for this worker."
    );
    assert(
      this._exchange,
      "Cannot start without a bound exchange. Please call .from(exchange) to bind to an exchange."
    );
    assert(
      this._perform,
      "Cannot start without a perform function. Please call .perform(fn) and provide a perform function."
    );
  }

  then(onFulfilled: (arg0: any) => any, onRejected: () => any) {
    onFulfilled(this.stopping);
  }

  async start(signal: AbortSignal): Promise<ValidatedWorker> {
    signal.throwIfAborted();
    this.#validate();
    const session = new Session(this);

    signal.addEventListener(
      "abort",
      () => {
        this.log("stopping...");
        const stop = async (): Promise<boolean> => {
          this.log(
            `${session.activeCount} in progress, ${session.pendingCount} pending`
          );
          if (session.busy /* && not too much time has passed */) {
            return new Promise((resolve) => {
              setTimeout(() => resolve(stop()), 500);
            });
          } else {
            await session.close();
            return true;
          }
        };
        this.stopping = stop();
      },
      { once: true }
    );

    await session.assert();

    try {
      await session.consume(signal);
      this.log(`started`);
    } catch (e) {
      this.emit("error", e);
    }

    return this;
  }

  async consume(channel: AmqpChannel, message: Message) {
    this.#validate();
    const {
      content,
      properties: { headers, messageId },
      properties,
      fields,
    } = message;

    try {
      const parsed = await this.parseMessage(content);
      this.log(`start ${messageId ? `messageId=${messageId} ` : ""}`);
      await this._perform(parsed, { ...message });
    } catch (e) {
      const retries = (headers["x-death"] || []).find(
        ({ queue }) => queue === this._retryQ.name
      );
      const retryCount = retries ? retries.count : 0;
      if (retryCount < this.options.retries) {
        await channel.sendToQueue(this._retryQ.name, content, {
          ...properties,
          ...fields,
          expiration:
            Math.ceil(
              retryCount ** 4 + 15 + Math.random() * 30 * (retryCount + 1)
            ) * 1000,
        });
      } else {
        await channel.sendToQueue(this._deadQ.name, content, {
          ...properties,
          ...fields,
          expiration: this.options.deadJobRetensionMs,
        });
      }
      this.emit("error", e, message);
    } finally {
      await channel.ack(message);
    }
  }

  log(...messages: any) {
    console.log(
      this.tag,
      new Date().toISOString(),
      this._queue ? `queue=${this._queue.name}` : "",
      ...messages
    );
  }
}

// export default Worker;

function workerId() {
  return [os.hostname(), process.pid, randomBytes(3).toString("hex")].join("-");
}
