import getPostgresDurabilityEngine, { POSTGRES_DURABILITY_ENGINE, } from "../engine/postgres"
import Message from "../entity/message"
import getMemoryDurabilityEngine, { MEMORY_DURABILITY_ENGINE } from "./memory"
import getFirestoreDurabilityEngine, { FIRESTORE_DURABILITY_ENGINE, } from "./firestore"

export const DURABILITY_ENGINE = "DURABILITY_ENGINE";

export type QueuesMap = Record<string, Message[]>;

type DurabilityEngine = {
  getQueues: () => QueuesMap | Promise<QueuesMap>;
  addMessage: (queueName: string, message: Message) => void | Promise<void>;
  updateMessage: (
    queueName: string,
    id: Message["id"],
    message: Omit<Message, "id">
  ) => void | Promise<void>;
  deleteMessage: (queueName: string, id: Message["id"]) => void | Promise<void>;
};

export default DurabilityEngine;

export async function getDurabilityEngine(
  engine: String = (process.env[DURABILITY_ENGINE] ?? "")?.toLowerCase()
): Promise<DurabilityEngine> {
  switch (engine) {
    case FIRESTORE_DURABILITY_ENGINE:
      return getFirestoreDurabilityEngine();
    case POSTGRES_DURABILITY_ENGINE:
      return getPostgresDurabilityEngine();
    case MEMORY_DURABILITY_ENGINE:
      return getMemoryDurabilityEngine();
    default:
      throw new Error(`${engine} is not recognized as a DurabilityEngine.`);
  }
}
