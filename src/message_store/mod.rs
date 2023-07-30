use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::ops::Add;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use strum_macros::Display;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::message_store::model::{Message, MessageStatus};

pub mod model;
pub mod postgres;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StatusKey {
    subscriber_id: String,
    message_id: String,
}

#[derive(Clone, Debug, Default)]
pub struct MessageQueue {
    items: VecDeque<Message>,
    status_key_to_status: HashMap<StatusKey, MessageStatus>,
}

#[derive(Default)]
pub struct MessageStore {
    in_memory_store: Arc<RwLock<HashMap<String, MessageQueue>>>,
    durable_store: Option<Box<dyn AsyncMessageStore + Send + Sync>>,
}

#[derive(Debug, Display)]
pub enum MessageStoreError {
    UnderlyingError { error: Box<dyn Error> },
    InvalidConfigError { message: String },
}

impl MessageStore {
    pub fn new() -> Self {
        MessageStore {
            in_memory_store: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            durable_store: None,
        }
    }

    pub fn new_with_durable_store(durable_store: Box<dyn AsyncMessageStore + Send + Sync>) -> Self {
        MessageStore {
            in_memory_store: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            durable_store: Some(durable_store),
        }
    }
}

impl MessageStore {
    pub async fn remove_expired(&self) -> Result<(), MessageStoreError> {
        let queue_name_to_expired_message_ids = self.find_expired_message_ids().await;

        self.remove_expired_from_memory_store(&queue_name_to_expired_message_ids)
            .await?;
        self.remove_expired_from_durable_store(
            &queue_name_to_expired_message_ids
                .iter()
                .flat_map(|(_, message_ids)| Vec::from_iter(message_ids))
                .collect(),
        )
        .await?;

        Ok(())
    }

    async fn find_expired_message_ids(&self) -> Vec<(String, HashSet<String>)> {
        self.in_memory_store
            .read()
            .await
            .iter()
            .map(|(queue_name, queue)| {
                (
                    queue_name.to_owned(),
                    HashSet::from_iter(
                        queue
                            .items
                            .iter()
                            .filter(|message| message.expiry.le(&Utc::now()))
                            .map(|message| message.id.to_owned()),
                    )
                    .to_owned(),
                )
            })
            .collect::<Vec<(String, HashSet<String>)>>()
    }

    async fn remove_expired_from_memory_store(
        &self,
        queue_name_to_expired_message_ids: &Vec<(String, HashSet<String>)>,
    ) -> Result<(), MessageStoreError> {
        let mut in_memory_store = self.in_memory_store.write().await;
        queue_name_to_expired_message_ids
            .iter()
            .for_each(|(queue_name, message_ids)| {
                if let Some(queue) = in_memory_store.get_mut(queue_name) {
                    queue
                        .items
                        .retain(|message| !message_ids.contains(&message.id))
                }
            });
        Ok(())
    }

    async fn remove_expired_from_durable_store(
        &self,
        expired_message_ids: &Vec<&String>,
    ) -> Result<(), MessageStoreError> {
        if let Some(durable_store) = &self.durable_store {
            for message_id in expired_message_ids {
                durable_store.remove(message_id).await?;
            }
        }
        Ok(())
    }
}

impl MessageStore {
    pub async fn initialize(&self) -> Result<(), MessageStoreError> {
        if let Some(durable_store) = &self.durable_store {
            let mut name_to_queue = self.in_memory_store.write().await;

            name_to_queue.clear();
            for (queue_name, queue) in durable_store.initialize().await? {
                name_to_queue.insert(queue_name, queue);
            }
        }
        Ok(())
    }

    pub async fn add(
        &self,
        queue_name: &str,
        value: &str,
        expiry: &DateTime<Utc>,
    ) -> Result<Message, MessageStoreError> {
        let new_message = match &self.durable_store {
            Some(durable_store) => durable_store.add(queue_name, value, expiry).await?,
            None => Message {
                id: Uuid::new_v4().to_string(),
                value: value.to_owned(),
                expiry: expiry.to_owned(),
            },
        };

        self.in_memory_store
            .write()
            .await
            .entry(queue_name.to_owned())
            .or_insert_with(MessageQueue::default)
            .items
            .push_back(new_message.clone());

        Ok(new_message)
    }

    pub async fn next(
        &self,
        queue_name: &str,
        subscriber_id: &str,
        acknowledgement_duration: Duration,
    ) -> Result<Option<Message>, MessageStoreError> {
        let mut in_memory_store = self.in_memory_store.write().await;

        if let Some(message_queue) = in_memory_store.get_mut(queue_name) {
            let maybe_next_message = message_queue
                .items
                .iter()
                .find(|message| {
                    if message.expiry.le(&Utc::now()) {
                        return false;
                    }

                    let status = message_queue
                        .status_key_to_status
                        .get(&StatusKey {
                            subscriber_id: subscriber_id.to_owned(),
                            message_id: message.id.to_owned(),
                        })
                        .unwrap_or(&MessageStatus::Available);
                    return match status {
                        MessageStatus::Available => true,
                        MessageStatus::Reserved(until) if until.le(&Utc::now()) => true,
                        _ => false,
                    };
                })
                .inspect(|message| {
                    message_queue.status_key_to_status.insert(
                        StatusKey {
                            subscriber_id: subscriber_id.to_owned(),
                            message_id: message.id.to_owned(),
                        },
                        MessageStatus::Reserved(Utc::now().add(acknowledgement_duration)),
                    );
                });

            return Ok(maybe_next_message.cloned());
        }

        Ok(None)
    }

    pub async fn acknowledge(
        &self,
        queue_name: &str,
        subscriber_id: &str,
        message_id: &str,
    ) -> Result<(), MessageStoreError> {
        if let Some(durable_store) = &self.durable_store {
            durable_store.acknowledge(subscriber_id, message_id).await?;
        }

        let mut in_memory_store = self.in_memory_store.write().await;
        if let Some(message_queue) = in_memory_store.get_mut(queue_name) {
            message_queue.status_key_to_status.insert(
                StatusKey {
                    subscriber_id: subscriber_id.to_owned(),
                    message_id: message_id.to_owned(),
                },
                MessageStatus::Acknowledged,
            );
        }

        Ok(())
    }
}

#[async_trait]
pub trait AsyncMessageStore {
    async fn initialize(&self) -> Result<HashMap<String, MessageQueue>, MessageStoreError>;
    async fn add(
        &self,
        queue_name: &str,
        value: &str,
        expiry: &DateTime<Utc>,
    ) -> Result<Message, MessageStoreError>;
    async fn acknowledge(
        &self,
        subscriber_id: &str,
        message_id: &str,
    ) -> Result<(), MessageStoreError>;
    async fn reserve(
        &self,
        subscriber_id: &str,
        message_id: &str,
        until: &DateTime<Utc>,
    ) -> Result<(), MessageStoreError>;
    async fn remove(&self, message_id: &str) -> Result<(), MessageStoreError>;
}
