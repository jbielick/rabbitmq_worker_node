import { describe, expect, test, jest } from "@jest/globals";
// import amqpMock from "../__mocks__/amqplib";
// jest.unstable_mockModule("amqplib", () => amqpMock);
// const amqp = await import("amqplib");
import amqp from "../__mocks__/amqplib";
jest.mock("node:os");
const { default: os } = await import("node:os");
import { Worker } from "../worker";
import { sleep } from "..";

describe("Logging", () => {
  beforeEach(() => {
    jest.spyOn(global.console, "log");
    jest.spyOn(global.console, "error");
  });
  afterEach(() => jest.restoreAllMocks());

  test("emits connection errors", async () => {
    const TestWorker = class extends Worker {};
    const conn = await amqp.connect();
    const connFactory = jest.fn(() => conn);
    const chan = await conn.createChannel();
    jest.spyOn(conn, "createChannel").mockImplementation(async () => chan);
    const worker = new TestWorker()
      .from("events")
      .on("error", console.error)
      .toConnect(connFactory)
      .perform(() => {});
    const ac = new AbortController();

    await worker.start(ac.signal);
    conn.emit("close", new Error("Socket closed by remote peer"));
    chan.emit("close");

    await sleep(100);

    expect(console.error).toMatchInlineSnapshot(`
      [MockFunction] {
        "calls": [
          [
            [Error: Socket closed by remote peer],
          ],
        ],
        "results": [
          {
            "type": "return",
            "value": undefined,
          },
        ],
      }
    `);
  });

  test("emits run loop errors", async () => {
    const TestWorker = class extends Worker {};
    const conn = await amqp.connect();
    const connFactory = jest.fn(() => conn);
    const chan = await conn.createChannel();
    jest.spyOn(conn, "createChannel").mockImplementation(async () => chan);
    const worker = new TestWorker()
      .from("events")
      .toConnect(connFactory)
      .on("error", console.error)
      .perform(() => {});
    const ac = new AbortController();

    await worker.start(ac.signal);

    jest.spyOn(chan, "consume").mockImplementationOnce(async () => {
      throw new Error("consume failed");
    });
    chan.emit("error", new Error("network error"));
    chan.emit("close");
    await sleep(10);
    expect(console.error).toMatchInlineSnapshot(`
      [MockFunction] {
        "calls": [
          [
            [Error: network error],
          ],
        ],
        "results": [
          {
            "type": "return",
            "value": undefined,
          },
        ],
      }
    `);
  });

  test("prefixes logs", async () => {
    jest.spyOn(os, "hostname").mockReturnValue("workerhost");
    const TestWorker = class extends Worker {};
    const conn = await amqp.connect();
    const payload = {};
    const worker = new TestWorker()
      .from("events")
      .toConnect(() => conn)
      .on("error", console.error)
      .perform((data) => {});
    const ac = new AbortController();

    await worker.start(ac.signal);
    await conn.push("events", Buffer.from(JSON.stringify(payload)));
    ac.abort();
    await worker;

    expect(console.log).toHaveBeenNthCalledWith(
      1,
      expect.stringMatching(new RegExp(`workerhost-${process.pid}-\\w{6}`)),
      expect.any(String),
      expect.stringMatching("queue=events"),
      expect.stringContaining("started")
    );
    expect(console.log).toHaveBeenNthCalledWith(
      2,
      expect.stringMatching(new RegExp(`workerhost-${process.pid}-\\w{6}`)),
      expect.any(String),
      expect.stringMatching("queue=events"),
      expect.stringContaining("stopping...")
    );
  });
});
