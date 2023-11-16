import { describe, expect, test, jest } from "@jest/globals";
// import amqpMock from "../__mocks__/amqplib";
// jest.unstable_mockModule("amqplib", () => amqpMock);
// const amqp = await import("amqplib");
import amqp from "../__mocks__/amqplib";
import { Worker } from "../worker";

describe("Retries", () => {
  beforeEach(() => {
    jest.spyOn(global.console, "log");
    jest.spyOn(global.console, "error");
  });
  afterEach(() => jest.restoreAllMocks());

  test("sends failed jobs to the retry queue with an expiration", async () => {
    const TestWorker = class extends Worker {};
    const conn = await amqp.connect();
    const connFactory = jest.fn(() => conn);
    const chan = await conn.createChannel();
    const payload = { id: 123 };
    jest.spyOn(conn, "createChannel").mockImplementation(async () => chan);
    const error = new Error("ouch");
    const buffer = Buffer.from(JSON.stringify(payload));
    const errHandler = jest.fn();
    const w = new TestWorker()
      .from("events")
      .toConnect(connFactory)
      .on("error", errHandler)
      .perform(() => {
        throw error;
      });
    const ac = new AbortController();

    await w.start(ac.signal);
    const message = await conn.push("events", buffer);
    ac.abort();
    await w;

    expect(chan.ack).toHaveBeenCalledTimes(w.options.retries + 1);
    expect(chan.ack).toHaveBeenCalledWith(message);
    expect(errHandler).toHaveBeenLastCalledWith(
      error,
      expect.objectContaining({
        content: buffer,
        properties: expect.objectContaining({
          expiration: expect.any(Number),
        }),
      })
    );
    expect(chan.sendToQueue).toHaveBeenCalledWith("events.retry", buffer, {
      ...message.properties,
      ...message.fields,
      expiration: expect.any(Number),
    });
    expect(
      ((chan.sendToQueue as jest.Mock).mock.calls[0][2] as typeof message)
        .expiration
    ).toBeGreaterThan(1000);
  });

  test("sends failed jobs to the dead queue after they exhaust retries", async () => {
    const TestWorker = class extends Worker {};
    const conn = await amqp.connect();
    const connFactory = jest.fn(() => conn);
    const chan = await conn.createChannel();
    const payload = { id: 123 };
    jest.spyOn(conn, "createChannel").mockImplementation(async () => chan);
    const error = new Error("ouch");
    const buffer = Buffer.from(JSON.stringify(payload));
    const perform = jest.fn(() => {
      throw error;
    });
    const w = new TestWorker()
      .from("events")
      .toConnect(connFactory)
      .with({ retries: 1 })
      .on("error", console.error)
      .perform(perform);
    const ac = new AbortController();

    await w.start(ac.signal);
    const message = await conn.push("events", buffer);
    ac.abort();
    await w;

    expect(chan.ack).toHaveBeenCalledTimes(2);
    expect(chan.ack).toHaveBeenCalledWith(message);
    expect(chan.sendToQueue).toHaveBeenCalledTimes(2);
    expect(chan.sendToQueue).toHaveBeenCalledWith("events.retry", buffer, {
      ...message.properties,
      ...message.fields,
      expiration: expect.any(Number),
    });
    expect(
      ((chan.sendToQueue as jest.Mock).mock.calls[0][2] as typeof message)
        .expiration
    ).toBeGreaterThan(1000);
    expect(chan.sendToQueue).toHaveBeenCalledWith("events.dead", buffer, {
      ...message.properties,
      ...message.fields,
      expiration: expect.any(Number),
    });
  });
});
