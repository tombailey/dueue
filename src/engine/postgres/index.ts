import DurabilityEngine, { QueuesMap } from "../"

import pg from "pg"
import Message from "../../entity/message"

export const POSTGRES_DURABILITY_ENGINE = "postgres";

class PostgresDurabilityEngine implements DurabilityEngine {
  private readonly pgPool: pg.Pool;

  constructor(pgPool: pg.Pool) {
    this.pgPool = pgPool;
  }

  async initialize() {
    const client = await this.pgPool.connect();
    try {
      await client.query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";');
      await client.query(
        `
          CREATE TABLE IF NOT EXISTS dueue(
            id UUID PRIMARY KEY,
            queue TEXT NOT NULL,
            body TEXT NOT NULL,
            expiry TIMESTAMP DEFAULT NULL,
            restore TIMESTAMP DEFAULT NULL
          );
        `
      );
    } finally {
      client.release();
    }
  }

  async getQueues(): Promise<QueuesMap> {
    const client = await this.pgPool.connect();
    try {
      await client.query("DELETE FROM dueue WHERE expiry <= NOW();");
      const { rows } = await client.query<Record<string, any>>(
        "SELECT * FROM dueue;"
      );

      return rows.reduce((queueMap: QueuesMap, row) => {
        const message: Message = {
          id: row["id"],
          body: row["body"],
          expiry: row["expiry"] === null ? undefined : row["expiry"],
          restore: row["restore"] === null ? undefined : row["restore"],
        };
        const queue = row["queue"];
        if (queue in queueMap) {
          queueMap[queue].push(message);
        } else {
          queueMap[queue] = [message];
        }
        return queueMap;
      }, {});
    } finally {
      client.release();
    }
  }

  async addMessage(queueName: string, message: Message): Promise<void> {
    const client = await this.pgPool.connect();
    try {
      await client.query(
        `
          INSERT INTO dueue (id, queue, body, expiry, restore) VALUES ($1, $2, $3, $4, $5);
        `,
        [
          message.id,
          queueName,
          message.body,
          message.expiry ?? null,
          message.restore ?? null,
        ]
      );
    } finally {
      client.release();
    }
  }

  async updateMessage(
    queueName: string,
    id: string,
    message: Omit<Message, "id">
  ): Promise<void> {
    const client = await this.pgPool.connect();
    try {
      await client.query(
        `
          UPDATE dueue SET body=$1, expiry=$2, restore=$3 WHERE id=$4 AND queue=$5;
        `,
        [
          message.body,
          message.expiry ?? null,
          message.restore ?? null,
          id,
          queueName,
        ]
      );
    } finally {
      client.release();
    }
  }

  async deleteMessage(queueName: string, id: string): Promise<void> {
    const client = await this.pgPool.connect();
    try {
      await client.query("DELETE FROM dueue WHERE queue=$1 AND id=$2;", [
        queueName,
        id,
      ]);
    } finally {
      client.release();
    }
  }
}

export default async function getPostgresDurabilityEngine(): Promise<DurabilityEngine> {
  const envVarToConfig: Record<string, string | undefined> = {
    host: process.env["POSTGRES_HOST"],
    port: process.env["POSTGRES_PORT"],
    database: process.env["POSTGRES_DATABASE"],
    user: process.env["POSTGRES_USER"],
    password: process.env["POSTGRES_PASSWORD"],
  };

  const config = Object.keys(envVarToConfig).reduce(
    (config: Record<string, string>, key) => {
      const value = envVarToConfig[key];
      if (value === undefined) {
        throw new Error(`${key} is required for PostgresDurabilityEngine.`);
      }
      config[key] = value;
      return config;
    },
    {}
  );

  const engine = new PostgresDurabilityEngine(
    new pg.Pool({
      ...config,
      port: parseInt(config.port),
    })
  );
  await engine.initialize();
  return engine;
}
