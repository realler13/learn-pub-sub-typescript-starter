import amqp from "amqplib";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

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

  const playingState: PlayingState = { isPaused: true };
  await publishJSON(ch, ExchangePerilDirect, PauseKey, playingState);
  console.log("Published paused state successfully.");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
