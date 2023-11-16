import { describe, expect, test, jest } from "@jest/globals";
// import amqpMock from "../__mocks__/amqplib";
// jest.unstable_mockModule("amqplib", () => amqpMock);
// const amqp = await import("amqplib");
import amqp from "../__mocks__/amqplib";
import { Exchange } from "../exchange";

describe("Exchange", () => {
  test("implements chaining for mutation", () => {
    const options = { "x-dead-letter-exchange": "dlx" };
    let q1 = new Exchange("events").with();

    let q2 = q1.with(options);

    expect(q1.options).toEqual({});
    expect(q2.options).toEqual(options);
  });

  test("accepts options in the initializer", () => {
    const options = { "x-dead-letter-exchange": "dlx" };
    let q1 = new Exchange("events", options);

    expect(q1.options).toEqual(options);
  });

  test("throws when exchange name is not a string", () => {
    expect(() => new Exchange({} as string)).toThrowErrorMatchingInlineSnapshot(
      `"exchange name must be a String"`
    );
  });

  test("allows setting type in .with", () => {
    let q = new Exchange("events");

    q = q.with({ type: "direct" });

    expect(q.type).toEqual("direct");
  });

  test("asserts itself on the provided channel", async () => {
    const name = "work";
    const options = { "x-dead-letter-exchange": "dlx" };
    const q = new Exchange(name).with(options);
    const conn = await amqp.connect();
    const ch = await conn.createChannel();

    await q.assert(ch);

    expect(ch.assertExchange).toBeCalledWith(name, "topic", options);
  });
});
