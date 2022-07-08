import DurabilityEngine, { QueuesMap } from "../"

import Message from "../../entity/message"

import { cert, initializeApp } from "firebase-admin/app"
import { getFirestore } from "firebase-admin/firestore"
import { firestore } from "firebase-admin"
import Firestore = firestore.Firestore

export const FIRESTORE_DURABILITY_ENGINE = "firestore";

class FirestoreDurabilityEngine implements DurabilityEngine {
  private readonly firestore: Firestore;
  private readonly collection: string;

  constructor(firestore: Firestore, collection: string) {
    this.firestore = firestore;
    this.collection = collection;
  }

  async getQueues(): Promise<QueuesMap> {
    const queueDocs = await this.firestore.collection(this.collection).get();

    return queueDocs.docs.reduce((queueMap: QueuesMap, document) => {
      const message: Message = {
        id: document.id,
        body: document.get("body"),
        expiry: document.get("expiry")
          ? document.get("expiry").toDate()
          : undefined,
        restore: document.get("restore")
          ? document.get("restore").toDate()
          : undefined,
      };
      const queue = document.get("queue");
      if (queue in queueMap) {
        queueMap[queue].push(message);
      } else {
        queueMap[queue] = [message];
      }
      return queueMap;
    }, {});
  }

  async addMessage(queueName: string, message: Message): Promise<void> {
    const data = {
      body: message.body,
      expiry: message.expiry ?? null,
      restore: message.restore ?? null,
    };

    await this.firestore
      .collection(this.collection)
      .doc(message.id)
      .set({
        ...data,
        queue: queueName,
      });
  }

  async updateMessage(
    queueName: string,
    id: string,
    message: Omit<Message, "id">
  ): Promise<void> {
    const data = {
      body: message.body,
      expiry: message.expiry ?? null,
      restore: message.restore ?? null,
    };

    await this.firestore
      .collection(this.collection)
      .doc(id)
      .set({
        ...data,
        queue: queueName,
      });
  }

  async deleteMessage(queueName: string, id: string): Promise<void> {
    await this.firestore.collection(this.collection).doc(id).delete();
  }
}

export default async function getFirestoreDurabilityEngine(): Promise<DurabilityEngine> {
  const envVarToConfig: Record<string, string | undefined> = {
    credentialsFile: process.env["FIRESTORE_CREDENTIALS_FILE"],
    collection: process.env["FIRESTORE_COLLECTION"],
  };

  const config = Object.keys(envVarToConfig).reduce(
    (config: Record<string, string>, key) => {
      const value = envVarToConfig[key];
      if (value === undefined) {
        throw new Error(`${key} is required for FirestoreDurabilityEngine.`);
      }
      config[key] = value;
      return config;
    },
    {}
  );

  initializeApp({
    credential: cert(require(config.credentialsFile)),
  });
  const firestore = getFirestore();

  return new FirestoreDurabilityEngine(firestore, config.collection);
}
