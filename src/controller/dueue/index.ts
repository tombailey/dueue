import DurabilityEngine, { QueuesMap } from "../../engine"
import Lock from "../../lock"
import Message from "../../entity/message"
import NewMessage from "../../entity/newMessage"

import { v4 as uuid } from "uuid"

export default class DueueController {
  private static readonly DEFAULT_ACKNOWLEDGEMENT_MS = 5 * 60 * 1000;

  private readonly queuesMap: QueuesMap;
  private readonly durabilityEngine: DurabilityEngine;
  private readonly lock: Lock;

  constructor(
    queuesMap: QueuesMap,
    durabilityEngine: DurabilityEngine,
    lock: Lock
  ) {
    this.queuesMap = queuesMap;
    this.durabilityEngine = durabilityEngine;
    this.lock = lock;
  }

  receiveOne(
    queueName: string,
    acknowledgementDeadline: Date = new Date(
      Date.now() + DueueController.DEFAULT_ACKNOWLEDGEMENT_MS
    )
  ): Promise<Message | null> {
    return this.lock.acquire(queueName, async () => {
      const messages = this.queuesMap[queueName] ?? [];
      for (let index = 0; index < messages.length; index++) {
        const message = messages[index];
        if (message.expiry && message.expiry.getTime() <= Date.now()) {
          try {
            await this.durabilityEngine.deleteMessage(queueName, message.id);
            console.info("A message expired.");
          } catch {
            console.warn("Failed to delete expired message.");
          }
          messages.splice(index, 1);
        } else if (message.restore === undefined) {
          await this.durabilityEngine.updateMessage(queueName, message.id, {
            ...message,
            restore: acknowledgementDeadline,
          });
          message.restore = acknowledgementDeadline;
          return message;
        }
      }
      return null;
    });
  }

  publishOne(queueName: string, newMessage: NewMessage): Promise<Message> {
    return this.lock.acquire(queueName, async () => {
      const message = {
        id: uuid(),
        body: newMessage.body,
        expiry: newMessage.expiry,
      };
      await this.durabilityEngine.addMessage(queueName, message);
      if (queueName in this.queuesMap) {
        this.queuesMap[queueName].push(message);
      } else {
        this.queuesMap[queueName] = [message];
      }
      return message;
    });
  }

  async acknowledgeOne(queueName: string, id: Message["id"]): Promise<void> {
    await this.lock.acquire(queueName, async () => {
      const messages = this.queuesMap[queueName] ?? [];
      const messageIndex = messages.findIndex((message) => message.id === id);

      await this.durabilityEngine.deleteMessage(queueName, id);
      messages.splice(messageIndex, 1);
    });
  }
}
