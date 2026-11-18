import amqp from "amqplib";
import {
  getInput,
  printServerHelp,
} from "../internal/gamelogic/gamelogic.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { writeLog, type GameLog } from "../internal/gamelogic/logs.js";
import {
  AckType,
  SimpleQueueType,
  subscribeMsgPack,
} from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import {
  ExchangePerilDirect,
  ExchangePerilTopic,
  GameLogSlug,
  PauseKey,
} from "../internal/routing/routing.js";

async function main() {
  console.log("Starting Peril server...");

  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected to RabbitMQ successfully.");
  const ch = await conn.createConfirmChannel();
  ch.on("error", (err) => {
    console.error("RabbitMQ channel error:", err);
  });

  async function shutdown() {
    console.log("Shutting down Peril server...");
    try {
      await conn.close();
      process.exit(0);
    } catch (err) {
      console.error("Failed to close RabbitMQ connection:", err);
      process.exit(1);
    }
  }

  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);

  await subscribeMsgPack(
    conn,
    ExchangePerilTopic,
    GameLogSlug,
    `${GameLogSlug}.*`,
    SimpleQueueType.Durable,
    async (gameLog: GameLog) => {
      try {
        await writeLog(gameLog);
        return AckType.Ack;
      } catch (err) {
        console.error("Failed to write game log:", err);
        return AckType.NackRequeue;
      } finally {
        process.stdout.write("> ");
      }
    },
  );
  console.log(`Subscribed to queue ${GameLogSlug}.`);

  // Used to run the server from a non-interactive source, like the multiserver.sh file
  if (!process.stdin.isTTY) {
    console.log("Non-interactive mode: skipping command input.");
    return;
  }

  printServerHelp();
  while (true) {
    const words = await getInput();
    const command = words[0];
    if (!command) {
      continue;
    }

    if (command === "pause" || command === "resume") {
      console.log(`Sending ${command} message...`);
      const playingState: PlayingState = { isPaused: command === "pause" };
      await publishJSON(ch, ExchangePerilDirect, PauseKey, playingState);
    } else if (command === "quit") {
      console.log("Exiting Peril server...");
      break;
    } else {
      console.log(`Unknown command: ${command}`);
    }
  }
  await shutdown();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
