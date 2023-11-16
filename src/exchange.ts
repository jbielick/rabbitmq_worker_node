import { AmqpChannel } from ".";

type ExchangeType = "topic" | "direct" | "fanout";

export interface ExchangeOptions {
  type?: ExchangeType;
  [key: string]: any;
}

export class Exchange {
  name;
  type: ExchangeType;
  #options = {};

  constructor(
    name: string,
    { type = "topic", ...options }: ExchangeOptions = {}
  ) {
    if (typeof name !== "string") {
      throw new Error("exchange name must be a String");
    }
    this.name = name;
    this.type = type;
    this.#with(options);
  }

  with(options = {}) {
    return this.#copy().#with(options);
  }

  #with(options: ExchangeOptions) {
    this.#options = {
      ...this.#options,
      ...options,
    };
    if (options.type) this.type = options.type;
    return this;
  }

  get options() {
    return { ...this.#options };
  }

  #copy() {
    return new Exchange(this.name).#with(this.#options);
  }

  assert(channel: AmqpChannel) {
    return channel.assertExchange(this.name, this.type, this.options);
  }
}
