import DurabilityEngine, { QueuesMap } from "../";

import Message from "../../entity/message";
import { createClient, SupabaseClient } from "@supabase/supabase-js";

export const SUPABASE_DURABILITY_ENGINE = "supabase";

type SavedMessage = Omit<
  Message,
  "expiry" | "acknowledgementDeadlines" | "acknowledgements"
> & {
  queue: string;
  expiry: string | null;
  acknowledgementDeadlines: string | null;
  acknowledgements: string | null;
};

class SupabaseDurabilityEngine implements DurabilityEngine {
  private readonly client: SupabaseClient;
  private readonly messageTable: string;

  constructor(client: SupabaseClient, messageTable: string) {
    this.client = client;
    this.messageTable = messageTable;
  }

  async getQueues(): Promise<QueuesMap> {
    const { data: messages, error } = await this.client
      .from<SavedMessage>(this.messageTable)
      .select();
    if (error) {
      throw error;
    } else {
      return messages.reduce((queueMap: QueuesMap, savedMessage) => {
        const queue = savedMessage.queue;
        const acknowledgementDeadlines = savedMessage.acknowledgementDeadlines
          ? JSON.parse(savedMessage.acknowledgementDeadlines)
          : undefined;
        const acknowledgements = savedMessage.acknowledgements
          ? new Set(JSON.parse(savedMessage.acknowledgements) as string[])
          : undefined;
        const expiry = savedMessage.expiry
          ? new Date(savedMessage.expiry)
          : undefined;
        const message = {
          ...savedMessage,
          expiry,
          acknowledgementDeadlines,
          acknowledgements,
          queue: undefined,
        };
        if (queue in queueMap) {
          queueMap[queue].push(message);
        } else {
          queueMap[queue] = [message];
        }
        return queueMap;
      }, {});
    }
  }

  async addMessage(queueName: string, message: Message): Promise<void> {
    const acknowledgementDeadlines = message.acknowledgementDeadlines
      ? JSON.stringify(message.acknowledgementDeadlines)
      : null;
    const acknowledgements = message.acknowledgements
      ? JSON.stringify(Array.from(message.acknowledgements))
      : null;
    const expiry = message.expiry ? message.expiry.toISOString() : undefined;
    const { error } = await this.client
      .from<SavedMessage>(this.messageTable)
      .insert({
        ...message,
        expiry,
        acknowledgementDeadlines,
        acknowledgements,
        queue: queueName,
      });
    if (error) {
      throw error;
    }
  }

  async updateMessage(
    queueName: string,
    id: string,
    message: Omit<Message, "id">
  ): Promise<void> {
    const acknowledgementDeadlines = message.acknowledgementDeadlines
      ? JSON.stringify(message.acknowledgementDeadlines)
      : null;
    const acknowledgements = message.acknowledgements
      ? JSON.stringify(Array.from(message.acknowledgements))
      : null;
    const expiry = message.expiry ? message.expiry.toISOString() : undefined;
    const { error } = await this.client
      .from<SavedMessage>(this.messageTable)
      .update({
        ...message,
        expiry,
        acknowledgementDeadlines,
        acknowledgements,
        queue: queueName,
      })
      .eq("id", id);
    if (error) {
      throw error;
    }
  }

  async deleteMessage(queueName: string, id: string): Promise<void> {
    const { error } = await this.client
      .from(this.messageTable)
      .delete()
      .eq("id", id);
    if (error) {
      throw error;
    }
  }
}

export default async function getSupabaseDurabilityEngine(): Promise<DurabilityEngine> {
  const envVarToConfig: Record<string, string | undefined> = {
    messageTable: process.env["SUPABASE_MESSAGE_TABLE"],
    url: process.env["SUPABASE_URL"],
    key: process.env["SUPABASE_KEY"],
  };

  const config = Object.keys(envVarToConfig).reduce(
    (config: Record<string, string>, key) => {
      const value = envVarToConfig[key];
      if (value === undefined) {
        throw new Error(`${key} is required for SupabaseDurabilityEngine.`);
      }
      config[key] = value;
      return config;
    },
    {}
  );

  const supabaseClient = createClient(config.url, config.key);
  return new SupabaseDurabilityEngine(supabaseClient, config.messageTable);
}
