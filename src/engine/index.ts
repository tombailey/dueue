import Message from "../entity/message";
import getMemoryDurabilityEngine, { MEMORY_DURABILITY_ENGINE } from "./memory";
import getFirestoreDurabilityEngine, {
  FIRESTORE_DURABILITY_ENGINE,
} from "./firestore";
import getSupabaseDurabilityEngine, {
  SUPABASE_DURABILITY_ENGINE,
} from "./supabase";

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
    case MEMORY_DURABILITY_ENGINE:
      return getMemoryDurabilityEngine();
    case SUPABASE_DURABILITY_ENGINE:
      return getSupabaseDurabilityEngine();
    default:
      throw new Error(`${engine} is not recognized as a DurabilityEngine.`);
  }
}
