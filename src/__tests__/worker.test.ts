import { describe, expect, test, jest } from "@jest/globals";
// import amqpMock from "../__mocks__/amqplib";
// jest.unstable_mockModule("amqplib", () => amqpMock);
// const amqp = await import("amqplib");
import amqp from "../__mocks__/amqplib";
import { ParserAlias, Worker } from "../worker";
import { AmqpConnection, sleep } from "..";

describe("Worker", () => {
  beforeEach(() => {
    jest.spyOn(global.console, "log");
    jest.spyOn(global.console, "error");
  });
  afterEach(() => jest.restoreAllMocks());

  test("implements chaining for mutation", () => {
    const worker = new Worker().with({ concurrency: 1, retries: 1 });
    expect(worker.options).toMatchObject({ concurrency: 1, retries: 1 });
    worker.with({ concurrency: 2, retries: 2 });
    expect(worker.options).toMatchObject({ concurrency: 2, retries: 2 });
    worker.with({ concurrency: 3, retries: 3 });
    expect(worker.options).toMatchObject({ concurrency: 3, retries: 3 });
  });

  test("does not allow starting without queue configuration", async () => {
    const worker = new Worker()
      .perform(() => {})
      .toConnect(() => ({} as unknown as AmqpConnection));

    expect(
      worker.start(new AbortController().signal)
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `"Cannot start without a queue. Please call .from(exchange) or .bind(queue) to setup a queue for this worker."`
    );
  });

  test("does not allow starting without an exchange configuration", async () => {
    const worker = new Worker()
      .bind("work")
      .perform(() => {})
      .toConnect(() => ({} as unknown as AmqpConnection));

    expect(
      worker.start(new AbortController().signal)
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `"Cannot start without a bound exchange. Please call .from(exchange) to bind to an exchange."`
    );
  });

  test("does not allow starting without perform function", async () => {
    const worker = new Worker()
      .from("events")
      .bind("work")
      .toConnect(() => ({} as unknown as AmqpConnection));

    expect(
      worker.start(new AbortController().signal)
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `"Cannot start without a perform function. Please call .perform(fn) and provide a perform function."`
    );
  });

  test("assumes an exchange type, queue name, and retry setup", async () => {
    const TestWorker = class extends Worker {};
    const conn = await amqp.connect();
    const worker = new TestWorker()
      .from("events")
      .toConnect(() => conn)
      .perform((data, ctx) => {});
    const ac = new AbortController();

    await worker.start(ac.signal);

    const chan = conn.channels![1];
    // source exchange
    expect(chan.assertExchange).toHaveBeenNthCalledWith(
      1,
      "events",
      "topic",
      {}
    );
    // requeue exchange to go directly to work queue
    expect(chan.assertExchange).toHaveBeenNthCalledWith(
      2,
      "events.requeue",
      "topic",
      {}
    );
    // retry queue with DLX to requeue
    expect(chan.assertQueue).toHaveBeenNthCalledWith(1, "events.retry", {
      deadLetterExchange: "events.requeue",
    });
    // dead queue
    expect(chan.assertQueue).toHaveBeenNthCalledWith(2, "events.dead", {});
    // work queue
    expect(chan.assertQueue).toHaveBeenNthCalledWith(3, "events", {});
    // events queue is bound to requeue exchange with #
    expect(chan.bindQueue).toHaveBeenNthCalledWith(
      1,
      "events",
      "events.requeue",
      "#"
    );
    // events queue is bound to events change with #
    expect(chan.bindQueue).toHaveBeenNthCalledWith(
      2,
      "events",
      "events",
      "#",
      {}
    );
    ac.abort();
    await worker;
  });

  test("allows naming the queue", async () => {
    const TestWorker = class extends Worker {};
    const conn = await amqp.connect();
    const worker = new TestWorker()
      .from("events")
      .bind("work")
      .toConnect(() => conn)
      .perform((data, ctx) => {});
    const ac = new AbortController();

    await worker.start(ac.signal);

    const chan = conn.channels![1];
    // source exchange
    expect(chan.assertExchange).toHaveBeenNthCalledWith(
      1,
      "events",
      "topic",
      {}
    );
    // requeue exchange to go directly to work queue
    expect(chan.assertExchange).toHaveBeenNthCalledWith(
      2,
      "work.requeue",
      "topic",
      {}
    );
    // retry queue with DLX to requeue
    expect(chan.assertQueue).toHaveBeenNthCalledWith(1, "work.retry", {
      deadLetterExchange: "work.requeue",
    });
    // dead queue
    expect(chan.assertQueue).toHaveBeenNthCalledWith(2, "work.dead", {});
    // work queue
    expect(chan.assertQueue).toHaveBeenNthCalledWith(3, "work", {});
    // events queue is bound to requeue exchange with #
    expect(chan.bindQueue).toHaveBeenNthCalledWith(
      1,
      "work",
      "work.requeue",
      "#"
    );
    // events queue is bound to events change with #
    expect(chan.bindQueue).toHaveBeenNthCalledWith(
      2,
      "work",
      "events",
      "#",
      {}
    );
    ac.abort();
    await worker;
  });

  test("restarts on connection error", async () => {
    const TestWorker = class extends Worker {};
    const conn = await amqp.connect();
    const connFactory = jest.fn(() => conn);
    const chan = await conn.createChannel();
    const createChan = jest
      .spyOn(conn, "createChannel")
      .mockImplementation(async () => chan);
    const worker = new TestWorker()
      .from("events")
      .toConnect(connFactory)
      .perform(() => {});
    const ac = new AbortController();

    await worker.start(ac.signal);

    // once internall, once above, once via start()
    expect(createChan).toHaveBeenCalledTimes(3);

    // interrupt 1
    chan.emit("close", new Error("closed via management plugin"));
    await sleep(100);
    expect(createChan).toHaveBeenCalledTimes(4);
    expect(chan.consume).toHaveBeenCalledTimes(2);

    // interrupt 2
    chan.emit("close", new Error("closed via management plugin"));
    await sleep(10);
    expect(createChan).toHaveBeenCalledTimes(5);
    expect(chan.consume).toHaveBeenCalledTimes(3);

    // // interrupt 3
    chan.emit("close", new Error("closed via management plugin"));
    await sleep(10);
    expect(createChan).toHaveBeenCalledTimes(6);
    expect(chan.consume).toHaveBeenCalledTimes(4);

    ac.abort();
    await worker;
  });

  test("restarts when initial channel consume startup error occurs", async () => {
    const TestWorker = class extends Worker {};
    const conn = await amqp.connect();
    const connFactory = jest.fn(() => conn);
    const chan = await conn.createChannel();
    const createChan = jest
      .spyOn(conn, "createChannel")
      .mockImplementation(async () => chan);
    jest.spyOn(chan, "consume").mockImplementationOnce(async () => {
      throw new Error("consume failed");
    });
    const worker = new TestWorker()
      .from("events")
      .toConnect(connFactory)
      .on("error", console.error)
      .perform(() => {});
    const ac = new AbortController();

    await worker.start(ac.signal);

    expect(connFactory).toHaveBeenCalledTimes(1);
    expect(chan.consume).toHaveBeenCalledTimes(2);
  });

  test("attempts startup three times successively before giving up", async () => {
    const TestWorker = class extends Worker {};
    const conn = await amqp.connect();
    const connFactory = jest.fn(() => conn);
    const chan = await conn.createChannel();
    jest.spyOn(conn, "createChannel").mockImplementation(async () => chan);
    jest.spyOn(chan, "consume").mockImplementation(async () => {
      throw new Error("consume failed");
    });
    const worker = new TestWorker()
      .from("events")
      .toConnect(connFactory)
      .on("error", console.error)
      .perform(() => {});
    const ac = new AbortController();

    await worker.start(ac.signal);

    expect(connFactory).toHaveBeenCalledTimes(1);
    expect(chan.consume).toHaveBeenCalledTimes(3);
  });

  test("parses JSON by default", async () => {
    const TestWorker = class extends Worker {};
    const conn = await amqp.connect();
    const payload = { my: "args" };
    const worker = new TestWorker()
      .from("events")
      .toConnect(() => conn)
      .on("error", console.error)
      .perform((data) => {
        expect(data).toEqual(payload);
      });
    const ac = new AbortController();

    await worker.start(ac.signal);
    await conn.push("events", Buffer.from(JSON.stringify(payload)));
    ac.abort();
    await worker;

    expect.assertions(1);
  });

  test("accepts a custom parser", async () => {
    const TestWorker = class extends Worker {};
    const conn = await amqp.connect();
    const payload = "()";
    const worker = new TestWorker()
      .from("events")
      .toConnect(() => conn)
      .parse((_) => "circle")
      .on("error", console.error)
      .perform((data) => {
        expect(data).toEqual("circle");
      });
    const ac = new AbortController();

    await worker.start(ac.signal);
    await conn.push("events", Buffer.from(payload));
    ac.abort();
    await worker;

    expect.assertions(1);
  });

  test("accepts a parser alias 'none' to indicate parsing behavior", async () => {
    const TestWorker = class extends Worker {};
    const conn = await amqp.connect();
    const payload = "()";
    const worker = new TestWorker()
      .from("events")
      .toConnect(() => conn)
      .parse("none")
      .on("error", console.error)
      .perform((data) => {
        expect(data.toString("utf8")).toEqual("()");
      });
    const ac = new AbortController();

    await worker.start(ac.signal);
    await conn.push("events", Buffer.from(payload));
    ac.abort();
    await worker;

    expect.assertions(1);
  });

  test("accepts a parser alias 'json' to indicate parsing behavior", async () => {
    const TestWorker = class extends Worker {};
    const conn = await amqp.connect();
    const payload = { id: 123 };
    const worker = new TestWorker()
      .from("events")
      .toConnect(() => conn)
      .parse("json")
      .on("error", console.error)
      .perform((data) => {
        expect(data).toEqual(payload);
      });
    const ac = new AbortController();

    await worker.start(ac.signal);
    await conn.push("events", Buffer.from(JSON.stringify(payload)));
    ac.abort();
    await worker;

    expect.assertions(1);
  });

  test("throws when an invalid parser alias is given", async () => {
    const TestWorker = class extends Worker {};

    expect(() => {
      new TestWorker().parse("image/png" as unknown as ParserAlias);
    }).toThrowErrorMatchingInlineSnapshot(
      `"unknown content type argument: image/png"`
    );
  });

  test("throws when no connect function is provided", async () => {
    const TestWorker = class extends Worker {};

    expect(
      new TestWorker()
        .from("events")
        .perform(() => {})
        .start(new AbortController().signal)
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `"Cannot start without a connection factory. Please call .toConnect(...) and provide a connection factory function."`
    );
  });

  test("allows defaults on the class level", async () => {
    const TestWorker = class extends Worker {};
    const connectFn = jest.fn<typeof amqp.connect>(amqp.connect);
    TestWorker.connect = connectFn;
    TestWorker.defaults = {
      retries: 100,
    };

    const worker = new TestWorker()
      .from("events")
      .parse("json")
      .on("error", console.error)
      .perform((data) => {});
    const ac = new AbortController();

    await worker.start(ac.signal);
    ac.abort();
    await worker;

    expect(connectFn).toHaveBeenCalled();
    expect(new TestWorker().options).toMatchObject({ retries: 100 });
  });

  test("throws when connection does not implement createChannel", async () => {
    const TestWorker = class extends Worker {};
    const worker = new TestWorker()
      .from("events")
      .toConnect(() => amqp as unknown as AmqpConnection)
      .parse("json")
      .on("error", console.error)
      .perform((data) => {});

    expect(
      worker.start(new AbortController().signal)
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `"must provide instance of amqplib.Connection"`
    );
  });
});
