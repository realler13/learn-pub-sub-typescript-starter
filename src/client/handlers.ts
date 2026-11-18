import type { ConfirmChannel } from "amqplib";
import type {
  ArmyMove,
  RecognitionOfWar,
} from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";
import { AckType } from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import {
  ExchangePerilTopic,
  WarRecognitionsPrefix,
} from "../internal/routing/routing.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return (ps: PlayingState) => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return AckType.Ack;
  };
}

export function handlerMove(
  gs: GameState,
  ch: ConfirmChannel,
): (move: ArmyMove) => Promise<AckType> {
  return async (move: ArmyMove) => {
    const outcome = handleMove(gs, move);
    let ackType = AckType.NackDiscard;
    if (outcome === MoveOutcome.Safe) {
      ackType = AckType.Ack;
    } else if (outcome === MoveOutcome.MakeWar) {
      const rw: RecognitionOfWar = {
        attacker: move.player,
        defender: gs.getPlayerSnap(),
      };
      try {
        await publishJSON(
          ch,
          ExchangePerilTopic,
          `${WarRecognitionsPrefix}.${gs.getUsername()}`,
          rw,
        );
        ackType = AckType.Ack;
      } catch (err) {
        console.error("Failed to publish war declaration:", err);
        ackType = AckType.NackRequeue;
      }
    }
    process.stdout.write("> ");
    return ackType;
  };
}

export function handlerWar(
  gs: GameState,
): (rw: RecognitionOfWar) => Promise<AckType> {
  return async (rw: RecognitionOfWar) => {
    const outcome = handleWar(gs, rw);
    let ackType: AckType;
    switch (outcome.result) {
      case WarOutcome.NotInvolved:
        ackType = AckType.NackRequeue;
        break;
      case WarOutcome.NoUnits:
        ackType = AckType.NackDiscard;
        break;
      case WarOutcome.OpponentWon:
      case WarOutcome.YouWon:
      case WarOutcome.Draw:
        ackType = AckType.Ack;
        break;
      default:
        console.error("Unknown war outcome:", outcome);
        ackType = AckType.NackDiscard;
    }
    process.stdout.write("> ");
    return ackType;
  };
}
