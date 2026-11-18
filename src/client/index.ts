import amqp, { type ConfirmChannel } from "amqplib";
import {
  clientWelcome,
  commandStatus,
  getInput,
  getMaliciousLog,
  printClientHelp,
  printQuit,
} from "../internal/gamelogic/gamelogic.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import type { GameLog } from "../internal/gamelogic/logs.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import {
  subscribeJSON,
  SimpleQueueType,
} from "../internal/pubsub/consume.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";
import {
  ArmyMovesPrefix,
  ExchangePerilDirect,
  ExchangePerilTopic,
  GameLogSlug,
  PauseKey,
  WarRecognitionsPrefix,
} from "../internal/routing/routing.js";
import { handlerMove, handlerPause, handlerWar } from "./handlers.js";

async function publishGameLog(
  ch: ConfirmChannel,
  username: string,
  message: string,
): Promise<void> {
  const gameLog: GameLog = {
    username,
    message,
    currentTime: new Date(),
  };
  await publishMsgPack(
    ch,
    ExchangePerilTopic,
    `${GameLogSlug}.${username}`,
    gameLog,
  );
  console.log("Game log published successfully.");
}

async function main() {
  console.log("Starting Peril client...");

  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected to RabbitMQ successfully.");
  const ch = await conn.createConfirmChannel();
  ch.on("error", (err) => {
    console.error("RabbitMQ channel error:", err);
  });

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
  const gs = new GameState(username);
  await subscribeJSON(
    conn,
    ExchangePerilDirect,
    `${PauseKey}.${username}`,
    PauseKey,
    SimpleQueueType.Transient,
    handlerPause(gs),
  );
  console.log(`Subscribed to queue ${PauseKey}.${username}.`);
  await subscribeJSON(
    conn,
    ExchangePerilTopic,
    `${ArmyMovesPrefix}.${username}`,
    `${ArmyMovesPrefix}.*`,
    SimpleQueueType.Transient,
    handlerMove(gs, ch),
  );
  console.log(`Subscribed to queue ${ArmyMovesPrefix}.${username}.`);
  await subscribeJSON(
    conn,
    ExchangePerilTopic,
    WarRecognitionsPrefix,
    `${WarRecognitionsPrefix}.*`,
    SimpleQueueType.Durable,
    handlerWar(gs, (initiator, message) => publishGameLog(ch, initiator, message)),
  );
  console.log(`Subscribed to shared queue ${WarRecognitionsPrefix}.`);

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
        case "move": {
          const move = commandMove(gs, words);
          await publishJSON(
            ch,
            ExchangePerilTopic,
            `${ArmyMovesPrefix}.${username}`,
            move,
          );
          console.log("Move published successfully.");
          break;
        }
        case "status":
          await commandStatus(gs);
          break;
        case "help":
          printClientHelp();
          break;
        case "spam": {
          const countWord = words[1];
          if (!countWord) {
            console.error("Usage: spam <count>");
            break;
          }
          const count = Number(countWord);
          if (!Number.isSafeInteger(count) || count <= 0) {
            console.error("Spam count must be a positive integer.");
            break;
          }
          const publications: Promise<void>[] = [];
          for (let i = 0; i < count; i++) {
            publications.push(publishGameLog(ch, username, getMaliciousLog()));
          }
          await Promise.all(publications);
          console.log(`Published ${count} game logs.`);
          break;
        }
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
