import type { Channel, Connection } from "amqplib";
import { Socket } from "net";

export const sleep = (ms: number) =>
  new Promise((resolve) => setTimeout(resolve, ms));

export interface AmqpConnection
  extends Pick<Connection, "close" | "connection" | "emit"> {
  createChannel: () => Promise<AmqpChannel>;
  once: (
    eventName: string | symbol,
    listener: (...args: any[]) => void
  ) => AmqpConnection;
  on: (
    eventName: string | symbol,
    listener: (...args: any[]) => void
  ) => AmqpConnection;
  connection: {
    serverProperties: Connection["connection"]["serverProperties"];
    stream?: Socket;
  };
  channels?: AmqpChannel[];
}

export interface AmqpChannel
  extends Pick<
    Channel,
    | "assertExchange"
    | "assertQueue"
    | "bindQueue"
    | "close"
    | "prefetch"
    | "consume"
    | "ack"
    | "sendToQueue"
    | "cancel"
    | "emit"
  > {
  connection?: AmqpConnection;
  once: (
    eventName: string | symbol,
    listener: (...args: any[]) => void
  ) => AmqpChannel;
}
