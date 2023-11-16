import { once } from "node:events";
import assert from "node:assert";
import { EventEmitter } from "node:events";
import type { Message } from "amqplib";
import pLimit, { LimitFunction } from "p-limit";
import { AmqpConnection, AmqpChannel, sleep } from ".";
import { ValidatedWorker } from "./worker";
import Debug from "debug";

const debug = Debug("worker:session");

export interface ConnectionFactoryFunction {
  (...args: any[]): Promise<AmqpConnection> | AmqpConnection;
}

export class Session extends EventEmitter {
  worker: ValidatedWorker;
  limit: LimitFunction;
  #restarts: number = 0;
  conn?: AmqpConnection;
  chan?: AmqpChannel;

  constructor(worker: ValidatedWorker) {
    super();
    this.worker = worker;
    this.limit = pLimit(worker.concurrency);
  }

  // get connected(): boolean {
  //   return !!this.conn && this.conn.connection.stream?.readyState === "open";
  // }

  async connect(): Promise<AmqpChannel> {
    if (!this.conn) {
      debug("connection.open");
      this.conn = await this.worker.createConnection();
      if (!("createChannel" in this.conn)) {
        throw new Error("must provide instance of amqplib.Connection");
      }
      this.conn.once("close", (err: unknown) => {
        debug("connection.close");
        if (!(err instanceof Error)) return;
        this.worker.emit("error", err);
        this.worker.log(`connection closed unexpectedly:`, err);
        this.conn = undefined;
        this.chan = undefined;
      });
    }
    return this.createChannel();
  }

  async createChannel(): Promise<AmqpChannel> {
    assert(this.conn);
    if (!this.chan) {
      debug("channel.open");
      this.chan = await this.conn.createChannel();
      this.chan
        .once("close", () => {
          debug("channel.close");
          this.chan = undefined;
        })
        .once("error", async (err) => {
          debug("channel.error", err.message);
          this.worker.emit("error", err);
          if (this.chan) await this.chan.close();
        });
    }
    return this.chan;
  }

  async assert() {
    const channel = await this.connect();
    await Promise.all([
      this.worker._exchange.assert(channel),
      this.worker._requeueX.assert(channel),
    ]);
    await Promise.all([
      this.worker._retryQ.assert(channel),
      this.worker._deadQ.assert(channel),
      this.worker._queue.assert(channel),
    ]);
    await Promise.all([
      channel.bindQueue(
        this.worker._queue.name,
        this.worker._requeueX.name,
        "#"
      ),
      channel.bindQueue(
        this.worker._queue.name,
        this.worker._exchange.name,
        this.worker._routingKey,
        this.worker._bindArgs
      ),
    ]);
    channel.prefetch(this.worker.options.concurrency);
  }

  get activeCount(): number {
    return this.limit.activeCount;
  }

  get pendingCount(): number {
    return this.limit.pendingCount;
  }

  get busy(): boolean {
    return this.limit.activeCount > 0 || this.limit.pendingCount > 0;
  }

  async consume(signal: AbortSignal) {
    signal.throwIfAborted();

    let cancel = async () => {};

    signal.addEventListener("abort", () => {
      debug("evt abort");
      cancel();
    });

    const keepConsuming = async (): Promise<void> => {
      if (signal.aborted) return;

      const channel = await this.connect();

      once(channel, "close", { signal })
        .then(async () => {
          cancel = async () => {};

          const restartDelay =
            process.env.NODE_ENV === "test"
              ? 0
              : Math.ceil(
                  this.#restarts ** 2 + 1 * (this.#restarts + 1) * 1000
                );

          if (signal.aborted) return;
          debug(`channel lost. backoff=${restartDelay / 1000}s`);
          await sleep(restartDelay);
          if (signal.aborted) return;
          keepConsuming();
        })
        .catch((err) => {
          if (err.code === "ABORT_ERR") return;
          throw err;
        });

      const useConsume = async (
        channel: AmqpChannel
      ): Promise<() => Promise<void>> => {
        debug("consume.start");
        const { consumerTag } = await channel.consume(
          this.worker._queue.name,
          (message) => {
            if (!message) return;
            this.limit<[AmqpChannel, Message], Promise<unknown>>(
              this.worker.consume.bind(this.worker),
              channel,
              message
            );
          },
          { consumerTag: this.worker.tag }
        );
        return async () => {
          await channel.cancel(consumerTag);
        };
      };

      try {
        cancel = await useConsume(channel);
        this.#restarts = 0;
      } catch (e) {
        debug(`consume.error: '${(e as Error).message}'`);
        this.#restarts += 1;
        if (this.#restarts < 3) {
          debug(`consume.retry ${this.#restarts}`);
          return keepConsuming();
        } else {
          debug(`consume.exhausted`);
          this.#restarts = 0;
          throw e;
        }
      }
    };

    return keepConsuming();
  }

  async close() {
    if (this.chan) await this.chan.close();
    if (this.conn?.channels?.filter((itself: any) => itself).length === 1) {
      this.worker.log("autoclosing connection");
      await this.conn.close();
    }
  }
}
