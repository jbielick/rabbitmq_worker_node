# rabbitmq_worker_node

This library provides a high-level API for consuming messages from a rabbitmq exchange with sidekiq-like behavior.

## Quickstart

Create a worker instance and use the configuration methods to indicate the exchange to bind to, a _perform function_ to process messages, an error handler, and provide a connection factory function.

Call `start` with an `AbortSignal` to start consuming messages and use the abort signal to stop the worker.

```js
import Worker from "rabbitmq-worker";
import amqplib from "amqplib";

Worker.toConnect(() => amqplib.connect(process.env.AMQP_URL));

const controller = new AbortController();
process.once("SIGINT", () => controller.abort());

await new Worker()
  .from("events")
  .perform(async (data, { fields, properties }) => {
    // ... do work
    // data is a *parsed* message
  })
  .on("error", console.error(err))
  .start(controller.signal);
```

## Configuration

### `.from(exchangeName: string)`

A worker must be configured with an _exchange_ to source messages from. Use `worker.from(exchangeName)` to set the exchange that the worker's queue will be bound to.

### `.bind(queueName: string, routingKey = "#", args: StringIndexed = {})`

By default, starting a worker asserts a _queue_ specifically for this worker. The default queue name is the same as the exchange name. You can indicate the queue name to be used if desired by calling `worker.bind(queueName)`.

### `.perform(fn)`

A worker must be given a _perform function_ which will be called for each message retrieved. Use `worker.perform(fn)` to provide a _perform function_. The _perform function_ can be sync or async.

### `.toConnect(connectionFactory: ConnectionFactoryFunction)`

A worker must have an amqp _connection factory function_. You can provide this globally by setting a connection factory function to the `Worker.connect` static property. It is also possible to provide the connection factory to a single worker instance with `worker.toConnect(factoryFunc)`. The factory function must return (or resolve with) an `amqplib` connection instance. This function will be called any time the worker needs to reconnect to the server.

### `.parse(contentType: ParserFunction | "json" | "none")`

All messages are assumed to be JSON data and will be parsed prior to being passed as an argument to your _perform function_. You can override this behavior by using `.parse("none")` to disable parsing or by providing your own parsing function with `.parse((buffer: Buffer) => any)`.

### `.with(options: PartialWorkerOptions = {})`

You can configure other worker behavior at the class level or instance level. For global default options, use `Worker.defaults = { ... }` and the provided settings will be defaults for all instances. For options specific to one instance, use `worker.with({ ... })` to set options for only that instance.

WorkerOptions is a type that looks like this:

```
interface WorkerOptions {
  tag?: string;                  # the consumerTag the worker will use when connecting
  concurrency: number;           # how many jobs may be in progress at any given moment
  retries: number;               # how many times a job may retry before dying
  parser: ParserFunction;        # a function that is used to parse a message before perform
  deadJobRetensionMs: number;    # how long should dead jobs remain in the dead queue
}
```

## Retries

Jobs retry a default of 25 times with an exponential backoff. When all retries are exhausted, the job is sent to the dead queue.

## Design

This library prioritizes a simple convention over configuration. A worker will assert the exchange it intends to bind a queue to, a work queue to hold its messages, a binding between these two, a retry queue to hold messages awaiting retry, a dead queue to hold messages that have exhausted retries, a requeue exchange, and a binding between the requeue exchange and the work queue to automatically requeue messages that have waited their retry period.

### Example 1: Default behavior

```js
await new Worker()
  .from("events")
  .perform(() => {})
  .start(signal);
```

The worker defined above will create a work queue named `events`. Since no queue name was explicitly provided, a queue name of `events` is assumed. A queue to contain retryable messages is created named `events.retry`. A queue to hold dead messages is created named `events.dead`. An `events.requeue` exchange is created in order to re-queue messages that wait their retry period. The `events` exchange will be bound to the `events` work queue. The `events.requeue` exchange will be bound to the `events` work queue.

### Example 2: Specified queue name

```js
await new Worker()
  .from("orders")
  .bind("provisioning")
  .perform(() => {})
  .start(signal);
```

The worker defined above will create a work queue named `provisioning`. A queue to contain retryable messages is created named `provisioning.retry`. A queue to hold dead messages is created named `provisioning.dead`. An `provisioning.requeue` exchange is created in order to re-queue messages that wait their retry period. The `orders` exchange will be bound to the `provisioning` work queue. The `provisioning.requeue` exchange will be bound to the `provisioning` work queue.

### How retries work

When a message is delivered to the worker,
and the perform function is successful,
the message is acknowledged.

When a message is delivered to the worker,
and an unexpected error occurs,
and the message is unable to be sent to the retry or dead queue,
the message is _not_ acknowledged.

When a message is delivered to the worker,
and an unexpected error occurs,
and the message is sent to the retry or dead queue,
the message is acknowledged.

When a message is sent to the retry queue,
the message will have an expiration set.

When a message in the retry queue expires,
the message is dead-lettered to the requeue exchange.

When a message's deaths meet or exceed the retry limit,
the message is sent to the dead queue with an expiration.

When a message in the dead queue expires,
the message is discarded.

```
    ┌───────────────────┐
    │                   │
    │     exchange      │
    │                   │           ┌────────────────────────┐
    └─────────┬─────────┘           │                        │
              │           ┌─────────┤    requeue exchange    │
              │           │         │                        │
              │           │         └──────────────▲─────────┘
           routing      retry                      │
              │           │                     expires
              │           │                        │
              │           │         ┌──────────────┴─────────┐
┌─────────────▼───────────▼─┐       │                        │
│                           │       │  retry queue with TTL  │
│        work queue         │       │                        │
│                           │       └──────────────▲─────────┘
└─────────────┬─────────────┘                      │
              │                                    │
              │                                   yes
              │                                    │
           consume                          ┌──────┴───────┐         ┌──────────────┐
              │                             │              │         │              │
              │                 ┌───fail────►  can retry?  ├───no────►  dead queue  │
┌─────────────▼─────────────┐   │           │              │         │              │
│                           │   │           └──────────────┘         └──────────────┘
│       worker.perform      ├───┤
│                           │   │
└───────────────────────────┘   │           ┌──────────────┐
                                │           │              │
                                └──succeed──►     ack!     │
                                            │              │
                                            └──────────────┘
```

## Contributing

Run tests with:
`npm test`

Run build with:
`npm run build`
