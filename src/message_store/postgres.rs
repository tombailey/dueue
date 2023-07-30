use std::boxed::Box;
use std::collections::HashMap;
use std::num::ParseIntError;

use anyhow::anyhow;
use async_trait::async_trait;
use bb8::{Pool, RunError};
use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, TimeZone, Utc};
use tokio_postgres::{NoTls, Row};

use crate::env::require_env_var_or;
use crate::message_store::model::MessageStatus;
use crate::message_store::MessageStoreError::InvalidConfigError;
use crate::message_store::{
    AsyncMessageStore, Message, MessageQueue, MessageStoreError, StatusKey,
};
use crate::result::ResultMapExtension;

pub struct PostgresMessageStore {
    client: Pool<PostgresConnectionManager<NoTls>>,
    message_table_name: String,
    acknowledgement_table_name: String,
    reservation_table_name: String,
}

pub fn require_config_env_var(name: &str) -> Result<String, MessageStoreError> {
    require_env_var_or(
        name,
        InvalidConfigError {
            message: format!("Missing required environment variable {name}."),
        },
    )
}

impl PostgresMessageStore {
    pub async fn create() -> Result<PostgresMessageStore, MessageStoreError> {
        let host = require_config_env_var("POSTGRES_HOST")?;
        let port = require_config_env_var("POSTGRES_PORT")?
            .parse::<u16>()
            .map_err(|_| InvalidConfigError {
                message: format!("Invalid environment variable POSTGRES_PORT."),
            })?;
        let user = require_config_env_var("POSTGRES_USER")?;
        let password = require_config_env_var("POSTGRES_PASSWORD")?;
        let database = require_config_env_var("POSTGRES_DATABASE")?;

        let connection_string = format!("postgres://{user}:{password}@{host}:{port}/{database}");

        let pool = Pool::builder()
            .max_size(10)
            .build(PostgresConnectionManager::new(
                connection_string.parse()?,
                NoTls,
            ))
            .await?;

        Ok(PostgresMessageStore::create_with(pool))
    }
}

trait ParameterizedCreation {
    fn create_with(pool: Pool<PostgresConnectionManager<NoTls>>) -> PostgresMessageStore;
}

impl ParameterizedCreation for PostgresMessageStore {
    fn create_with(pool: Pool<PostgresConnectionManager<NoTls>>) -> PostgresMessageStore {
        PostgresMessageStore {
            client: pool,
            message_table_name: "dueue_message".to_owned(),
            acknowledgement_table_name: "dueue_acknowledgement".to_owned(),
            reservation_table_name: "dueue_reservation".to_owned(),
        }
    }
}

impl PostgresMessageStore {
    async fn ensure_tables_exist(&self) -> Result<(), MessageStoreError> {
        let client = self.client.get().await?;

        let message_table = &self.message_table_name;
        client
            .simple_query(&format!(
                "
                CREATE TABLE IF NOT EXISTS {message_table} (
                    id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    queue_name text,
                    value text,
                    expiry bigint
                );
            ",
            ))
            .await?;

        let acknowledgement_table = &self.acknowledgement_table_name;
        client
            .simple_query(&format!(
                "
                CREATE TABLE IF NOT EXISTS {acknowledgement_table} (
                    id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    message_id bigint,
                    subscriber_id text,
                    FOREIGN KEY(message_id) REFERENCES {message_table} (id)
                );
            "
            ))
            .await?;

        let reservation_table = &self.reservation_table_name;
        client
            .simple_query(&format!(
                "
                CREATE TABLE IF NOT EXISTS {reservation_table} (
                    id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    message_id bigint,
                    subscriber_id text,
                    until bigint,
                    FOREIGN KEY(message_id) REFERENCES {message_table} (id)
                );
            "
            ))
            .await?;

        Ok(())
    }

    async fn remove_expired(&self) -> Result<(), MessageStoreError> {
        let message_table = &self.message_table_name;
        self.client
            .get()
            .await?
            .execute(
                &format!("DELETE FROM {message_table} WHERE expiry < $1;"),
                &[&Utc::now().timestamp_millis()],
            )
            .await?;

        Ok(())
    }
}

struct Acknowledgement {
    queue_name: String,
    subscriber_id: String,
    message_id: String,
}

struct Reservation {
    queue_name: String,
    subscriber_id: String,
    message_id: String,
    until: DateTime<Utc>,
}

impl PostgresMessageStore {
    async fn get_messages(&self) -> Result<HashMap<String, MessageQueue>, MessageStoreError> {
        let message_table = &self.message_table_name;
        let mut queue_name_to_queue = HashMap::new();
        self.client
            .get()
            .await?
            .query(
                &format!("SELECT * FROM {message_table} message ORDER BY expiry ASC;"),
                &[],
            )
            .await?
            .iter()
            .for_each(|row| {
                queue_name_to_queue
                    .entry(row.get("queue_name"))
                    .or_insert_with(MessageQueue::default)
                    .items
                    .push_back(row.into());
            });

        Ok(queue_name_to_queue)
    }

    async fn get_acknowledgements(&self) -> Result<Vec<Acknowledgement>, MessageStoreError> {
        let message_table = &self.message_table_name;
        let acknowledgement_table = &self.acknowledgement_table_name;
        Ok(self
            .client
            .get()
            .await?
            .query(
                &format!(
                    "
                    SELECT
                        acknowledgement.message_id AS message_id,
                        acknowledgement.subscriber_id AS subscriber_id,
                        message.queue_name AS queue_name
                    FROM {acknowledgement_table} acknowledgement
                    INNER JOIN {message_table} message ON acknowledgement.message_id=message.id;
                "
                ),
                &[],
            )
            .await?
            .iter()
            .map(|row| Acknowledgement {
                queue_name: row.get("queue_name"),
                subscriber_id: row.get("subscriber_id"),
                message_id: row.get("message_id"),
            })
            .collect())
    }

    async fn get_reservations(&self) -> Result<Vec<Reservation>, MessageStoreError> {
        let message_table = &self.message_table_name;
        let reservation_table = &self.reservation_table_name;
        Ok(self
            .client
            .get()
            .await?
            .query(
                &format!(
                    "
                    SELECT
                        reservation.message_id AS message_id,
                        reservation.subscriber_id AS subscriber_id,
                        reservation.until AS until,
                        message.queue_name AS queue_name
                    FROM {reservation_table} reservation
                    INNER JOIN {message_table} message ON reservation.message_id=message.id;
                "
                ),
                &[],
            )
            .await?
            .iter()
            .map(|row| Reservation {
                queue_name: row.get("queue_name"),
                subscriber_id: row.get("subscriber_id"),
                message_id: row.get("message_id"),
                until: Utc.timestamp_millis_opt(row.get("until")).unwrap(),
            })
            .collect())
    }
}

#[async_trait]
impl AsyncMessageStore for PostgresMessageStore {
    async fn initialize(&self) -> Result<HashMap<String, MessageQueue>, MessageStoreError> {
        self.ensure_tables_exist().await?;
        self.remove_expired().await?;

        let mut queue_name_to_queue = self.get_messages().await?;
        self.get_acknowledgements()
            .await?
            .iter()
            .for_each(|acknowledgement| {
                let queue_name: String = acknowledgement.queue_name.to_owned();
                if let Some(queue) = queue_name_to_queue.get_mut(&queue_name) {
                    queue.status_key_to_status.insert(
                        StatusKey {
                            subscriber_id: acknowledgement.subscriber_id.to_owned(),
                            message_id: acknowledgement.message_id.to_owned(),
                        },
                        MessageStatus::Acknowledged,
                    );
                }
            });

        self.get_reservations()
            .await?
            .iter()
            .for_each(|reservation| {
                let queue_name: String = reservation.queue_name.to_owned();
                if let Some(queue) = queue_name_to_queue.get_mut(&queue_name) {
                    queue.status_key_to_status.insert(
                        StatusKey {
                            subscriber_id: reservation.subscriber_id.to_owned(),
                            message_id: reservation.message_id.to_owned(),
                        },
                        MessageStatus::Reserved(reservation.until),
                    );
                }
            });

        Ok(queue_name_to_queue)
    }

    async fn add(
        &self,
        queue_name: &str,
        value: &str,
        expiry: &DateTime<Utc>,
    ) -> Result<Message, MessageStoreError> {
        let message_table = &self.message_table_name;
        self.client
            .get()
            .await?
            .query(
                &format!(
                    "INSERT INTO {message_table} (queue_name, value, expiry) VALUES ($1, $2, $3) RETURNING *;"
                ),
                &[
                    &queue_name,
                    &value,
                    &expiry.timestamp_millis(),
                ],
            )
            .await?
            .iter()
            .map(|row| row.to_owned().into())
            .next()
            .ok_or(
                MessageStoreError::UnderlyingError {
                    error: anyhow!("Failed to insert message.").into()
                }
            )
    }

    async fn acknowledge(
        &self,
        subscriber_id: &str,
        message_id: &str,
    ) -> Result<(), MessageStoreError> {
        let mut client = self.client.get().await?;
        let transaction = client.transaction().await?;

        let acknowledgement_table = &self.acknowledgement_table_name;
        transaction
            .execute(
                &format!("INSERT INTO {acknowledgement_table} (message_id, subscriber_id) VALUES ($1, $2);"),
                &[&message_id.parse::<i64>()?, &subscriber_id],
            )
            .await?;

        let reservation_table = &self.reservation_table_name;
        transaction
            .execute(
                &format!("DELETE FROM {reservation_table} WHERE message_id=$1;"),
                &[&message_id.parse::<i64>()?],
            )
            .await?;

        transaction.commit().await?;

        Ok(())
    }

    async fn reserve(
        &self,
        subscriber_id: &str,
        message_id: &str,
        until: &DateTime<Utc>,
    ) -> Result<(), MessageStoreError> {
        let reservation_table = &self.reservation_table_name;
        self.client
            .get()
            .await?
            .execute(
                &format!("INSERT INTO {reservation_table} (message_id, subscriber_id, until) VALUES ($1, $2, $3);"),
                &[&message_id.parse::<i64>()?, &subscriber_id, &until.timestamp_millis()],
            )
            .await
            .map_to_unit()
            .map_err(|database_error| database_error.into())
    }

    async fn remove(&self, message_id: &str) -> Result<(), MessageStoreError> {
        let message_table = &self.message_table_name;
        self.client
            .get()
            .await?
            .execute(
                &format!("DELETE * FROM {message_table} WHERE message_id=$1;"),
                &[&message_id.parse::<i64>()?],
            )
            .await
            .map_to_unit()
            .map_err(|database_error| database_error.into())
    }
}

impl Into<Message> for &Row {
    fn into(self) -> Message {
        let id: i64 = self.get("id");
        Message {
            id: id.to_string(),
            value: self.get("value"),
            expiry: Utc.timestamp_millis_opt(self.get("expiry")).unwrap(),
        }
    }
}

impl From<ParseIntError> for MessageStoreError {
    fn from(error: ParseIntError) -> MessageStoreError {
        MessageStoreError::UnderlyingError {
            error: Box::new(error),
        }
    }
}

impl From<tokio_postgres::error::Error> for MessageStoreError {
    fn from(error: tokio_postgres::error::Error) -> MessageStoreError {
        MessageStoreError::UnderlyingError {
            error: Box::new(error),
        }
    }
}

impl From<RunError<tokio_postgres::Error>> for MessageStoreError {
    fn from(error: RunError<tokio_postgres::Error>) -> MessageStoreError {
        MessageStoreError::UnderlyingError {
            error: Box::new(error),
        }
    }
}
