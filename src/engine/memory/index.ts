import DurabilityEngine, { QueuesMap } from "../";

import Message from "../../entity/message";

export const MEMORY_DURABILITY_ENGINE = "memory";

class InMemoryDurabilityEngine implements DurabilityEngine {
  private readonly queuesMap: Record<string, Message[]> = {};

  constructor(queuesMap: Record<string, Message[]> = {}) {
    this.queuesMap = queuesMap;
  }

  addMessage(queueName: string, message: Message): void | Promise<void> {
    if (queueName in this.queuesMap) {
      this.queuesMap[queueName].push(message);
    } else {
      this.queuesMap[queueName] = [message];
    }
  }

  deleteMessage(queueName: string, id: Message["id"]): void | Promise<void> {
    if (queueName in this.queuesMap) {
      this.queuesMap[queueName].filter((message) => message.id !== id);
    }
  }

  getQueues(): QueuesMap | Promise<QueuesMap> {
    return { ...this.queuesMap };
  }

  updateMessage(
    queueName: string,
    id: Message["id"],
    message: Omit<Message, "id">
  ): void | Promise<void> {
    if (queueName in this.queuesMap) {
      const messageIndex = this.queuesMap[queueName].findIndex(
        (message) => message.id === id
      );
      this.queuesMap[queueName][messageIndex] = {
        ...message,
        id,
      };
    }
  }
}

export default function getMemoryDurabilityEngine(): DurabilityEngine {
  return new InMemoryDurabilityEngine();
}
