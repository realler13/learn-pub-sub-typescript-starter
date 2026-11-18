import { encode } from "@msgpack/msgpack";
import type { ConfirmChannel } from "amqplib";

export function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const content = Buffer.from(JSON.stringify(value));
    ch.publish(
      exchange,
      routingKey,
      content,
      { contentType: "application/json" },
      (err) => {
        if (err) {
          reject(err);
          return;
        }
        resolve();
      },
    );
  });
}

export function publishMsgPack<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const content = Buffer.from(encode(value));
    ch.publish(
      exchange,
      routingKey,
      content,
      { contentType: "application/x-msgpack" },
      (err) => {
        if (err) {
          reject(err);
          return;
        }
        resolve();
      },
    );
  });
}
