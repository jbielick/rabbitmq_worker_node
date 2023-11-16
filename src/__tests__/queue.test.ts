import { describe, expect, jest, test } from "@jest/globals";
import amqp from "../__mocks__/amqplib";
import { Queue } from "../queue";

describe("Queue", () => {
  test("implements chaining for mutation", () => {
    const options = { "x-dead-letter-exchange": "dlx" };
    let q1 = new Queue("work").with();

    let q2 = q1.with(options);
    let q3 = q2.with({ messageTtl: "100" });

    expect(q1.options).toEqual({});
    expect(q2.options).toEqual(options);
    expect(q3.options).toEqual({ ...options, messageTtl: "100" });
  });

  test("throws when name is not a string", () => {
    expect(() => new Queue({} as string)).toThrowErrorMatchingInlineSnapshot(
      `"queue name must be a String"`
    );
  });

  test("asserts itself on the provided channel", async () => {
    const name = "work";
    const options = { "x-dead-letter-exchange": "dlx" };
    const q = new Queue(name).with(options);
    const conn = await amqp.connect();
    const ch = await conn.createChannel();

    await q.assert(ch);

    expect(ch.assertQueue).toBeCalledWith(name, options);
  });
});
