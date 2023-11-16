import { jest } from "@jest/globals";
import type { ConsumeMessage, Options } from "amqplib";
import { Socket } from "node:net";
import { AmqpChannel, AmqpConnection } from "..";
import EventEmitter from "node:events";

interface StringIndexed {
  [key: string]: any;
}

interface ConsumeFunction {
  (msg: ConsumeMessage | null): void;
}

interface MockConnection extends AmqpConnection {
  push(queue: string, data: Buffer): Promise<any>;
}

export default {
  connect: async () => {
    const channels: AmqpChannel[] = [];
    let queued: {
      [queue: string]: ConsumeMessage[];
    } = {};
    let consumers: {
      [queue: string]: ConsumeFunction;
    } = {};
    const push = (
      queue: string,
      data: Buffer,
      options: Options.Publish = {}
    ) => {
      const message = {
        content: data,
        properties: {
          contentType: options.contentType || "",
          contentEncoding: options.contentEncoding || "",
          headers: options.headers || {},
          deliveryMode: options.deliveryMode || "",
          correlationId: options.correlationId || "",
          priority: options.priority || "",
          replyTo: options.replyTo || "",
          expiration: options.expiration || "",
          messageId: options.messageId || "",
          timestamp: options.timestamp || Date.now(),
          userId: options.userId || "",
          type: options.type || "",
          appId: options.appId || "",
          clusterId: "test",
        },
        fields: {
          consumerTag: "",
          deliveryTag: 1,
          redelivered: false,
          exchange: "",
          routingKey: "",
        },
      };
      return new Promise((resolve) => {
        setImmediate(() => {
          if (consumers[queue]) {
            consumers[queue](message);
          } else {
            if (!queued[queue]) queued[queue] = [];
            queued[queue].push(message);
          }
          resolve(message);
        });
      });
    };
    const connection: MockConnection = Object.assign(new EventEmitter(), {
      channels,
      push,
      createChannel: jest.fn<AmqpConnection["createChannel"]>(async () => {
        const channel = Object.assign(new EventEmitter(), {
          ack: jest.fn<AmqpChannel["ack"]>(),
          sendToQueue: jest.fn<AmqpChannel["sendToQueue"]>(
            (queue, content, options = {}) => {
              if (queue.endsWith(".retry")) {
                if (!options.headers) options.headers = {};
                if (!options.headers["x-death"]) {
                  options.headers["x-death"] = [];
                }
                const retryDeath = options.headers["x-death"].find(
                  (death: { queue: string }) => death.queue === queue
                );
                if (retryDeath) {
                  retryDeath.count += 1;
                } else {
                  options.headers["x-death"].push({ queue: queue, count: 1 });
                }
                queue = queue.replace(/\.retry$/, "");
              }
              push(queue, content, options);
              return true;
            }
          ),
          prefetch: jest.fn<AmqpChannel["prefetch"]>(),
          assertExchange: jest.fn<AmqpChannel["assertExchange"]>(),
          assertQueue: jest.fn<AmqpChannel["assertQueue"]>(),
          bindQueue: jest.fn<AmqpChannel["bindQueue"]>(),
          consume: jest.fn<AmqpChannel["consume"]>(
            async (
              queue: string,
              fn: ConsumeFunction,
              options: StringIndexed = {}
            ) => {
              consumers[queue] = fn;
              return new Promise((resolve) => {
                setImmediate(() => {
                  resolve({
                    consumerTag: options.consumerTag || "TestConsumer",
                  });
                  if (queued[queue]) queued[queue].forEach(fn);
                });
              });
            }
          ),
          cancel: jest.fn<AmqpChannel["cancel"]>(),
          close: jest.fn<AmqpChannel["close"]>(async () => {
            channels.splice(channels.indexOf(channel), 1);
            channel.emit("close");
          }),
          connection,
        });
        channels.push(channel);
        return channel;
      }),
      close: jest.fn<AmqpConnection["close"]>(async () => {
        channels.forEach((chan) => chan.emit("close"));
        while (channels.length) {
          const chan = channels.pop();
          chan!.close();
        }
        connection.emit("close");
      }),
      connection: {
        stream: new Socket(),
        serverProperties: {
          host: "localhost",
          product: "rabbitmq",
          version: "3.5",
          platform: "linux/amd64",
          information: "none",
        },
      },
    });
    // 1 internal channel is dedicated to assertions or something
    await connection.createChannel();
    return connection;
  },
};
