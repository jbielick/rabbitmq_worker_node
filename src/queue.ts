import { AmqpChannel } from ".";

export interface QueueOptions extends StringIndexed {}

export interface PartialQueueOptions extends Partial<QueueOptions> {}

export class Queue {
  name;
  #options = {};

  constructor(name: string) {
    if (typeof name !== "string") {
      throw new Error("queue name must be a String");
    }
    this.name = name;
  }

  with(options = {}) {
    return this.#copy().#with(options);
  }

  #with(options: PartialQueueOptions) {
    this.#options = {
      ...this.#options,
      ...options,
    };
    return this;
  }

  get options() {
    return { ...this.#options };
  }

  #copy() {
    return new Queue(this.name).#with(this.#options);
  }

  assert(channel: AmqpChannel) {
    return channel.assertQueue(this.name, this.options);
  }
}
