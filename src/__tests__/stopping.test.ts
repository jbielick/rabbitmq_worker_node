import { describe, expect, test, jest } from "@jest/globals";
// import amqpMock from "../__mocks__/amqplib";
// jest.unstable_mockModule("amqplib", () => amqpMock);
// const amqp = await import("amqplib");
import amqp from "../__mocks__/amqplib";
import { Worker } from "../worker";

describe("Stopping", () => {
  beforeEach(() => {
    jest.spyOn(global.console, "log");
    jest.spyOn(global.console, "error");
  });
  afterEach(() => jest.restoreAllMocks());

  test("gracefully stops allowing in-progress jobs to finish", async () => {
    const TestWorker = class extends Worker {};
    const conn = await amqp.connect();
    const chan = await conn.createChannel();
    const payload = { id: 123 };
    jest.spyOn(conn, "createChannel").mockImplementation(async () => chan);
    const done = jest.fn();
    const worker = new TestWorker()
      .from("events")
      .toConnect(() => conn)
      .on("error", console.error)
      .perform(async (data) => {
        await new Promise((resolve) => setTimeout(resolve, 500));
        done();
      });
    const ac = new AbortController();

    await worker.start(ac.signal);
    const message = await conn.push(
      "events",
      Buffer.from(JSON.stringify(payload))
    );
    expect(chan.ack).not.toHaveBeenCalled();
    ac.abort();
    expect(chan.cancel).toHaveBeenCalled();
    expect(chan.close).not.toHaveBeenCalled();

    await worker;

    expect(chan.close).toHaveBeenCalled();
    expect(conn.close).toHaveBeenCalled();
    expect(done).toHaveBeenCalled();
  });
});
