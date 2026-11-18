import type { Channel, ChannelModel, ConsumeMessage, Replies } from "amqplib";
import { ExchangePerilDeadLetter } from "../routing/routing.js";

export enum SimpleQueueType {
  Durable,
  Transient,
}

export enum AckType {
  Ack,
  NackRequeue,
  NackDiscard,
}

export async function declareAndBind(
  conn: ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, Replies.AssertQueue]> {
  const ch = await conn.createChannel();
  const queue = await ch.assertQueue(queueName, {
    durable: queueType === SimpleQueueType.Durable,
    autoDelete: queueType === SimpleQueueType.Transient,
    exclusive: queueType === SimpleQueueType.Transient,
    arguments: {
      "x-dead-letter-exchange": ExchangePerilDeadLetter,
    },
  });
  await ch.bindQueue(queue.queue, exchange, key);
  return [ch, queue];
}

export async function subscribeJSON<T>(
  conn: ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  const [ch, queue] = await declareAndBind(
    conn,
    exchange,
    queueName,
    key,
    queueType,
  );
  await ch.consume(
    queue.queue,
    async (message: ConsumeMessage | null) => {
      if (message === null) {
        return;
      }
      const data: T = JSON.parse(message.content.toString());
      const ackType = await handler(data);
      switch (ackType) {
        case AckType.Ack:
          ch.ack(message);
          break;
        case AckType.NackRequeue:
          ch.nack(message, false, true);
          break;
        case AckType.NackDiscard:
          ch.nack(message, false, false);
          break;
      }
    },
    { noAck: false },
  );
}
