import amqp from "amqplib";
import {
  clientWelcome,
  commandStatus,
  getInput,
  printClientHelp,
  printQuit,
} from "../internal/gamelogic/gamelogic.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import {
  declareAndBind,
  SimpleQueueType,
} from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
  console.log("Starting Peril client...");

  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected to RabbitMQ successfully.");

  async function shutdown() {
    console.log("Shutting down Peril client...");
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

  const username = await clientWelcome();
  const [, queue] = await declareAndBind(
    conn,
    ExchangePerilDirect,
    `${PauseKey}.${username}`,
    PauseKey,
    SimpleQueueType.Transient,
  );
  console.log(`Declared and bound queue ${queue.queue}.`);
  const gs = new GameState(username);

  while (true) {
    const words = await getInput();
    const command = words[0];
    if (!command) {
      continue;
    }
    if (command === "quit") {
      printQuit();
      break;
    }

    try {
      switch (command) {
        case "spawn":
          commandSpawn(gs, words);
          break;
        case "move":
          commandMove(gs, words);
          break;
        case "status":
          await commandStatus(gs);
          break;
        case "help":
          printClientHelp();
          break;
        case "spam":
          console.log("Spamming not allowed yet!");
          break;
        default:
          console.error(`Unknown command: ${command}`);
      }
    } catch (err) {
      if (err instanceof Error) {
        console.error(err.message);
      } else {
        throw err;
      }
    }
  }
  await shutdown();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
