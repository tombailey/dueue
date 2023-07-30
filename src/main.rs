#![feature(future_join)]
#![feature(result_option_inspect)]
extern crate core;

use std::ops::Add;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};
use log::warn;
use tokio::sync::RwLock;
use tokio::time::Instant;

use crate::env::{env_var_or_default, require_env_var};
use crate::message_store::model::DurabilityEngine;
use crate::message_store::postgres::PostgresMessageStore;
use crate::message_store::{MessageStore, MessageStoreError};
use crate::router::{acknowledge_message, add_message, get_health, receive_message};

mod env;
mod message_store;
mod result;
mod router;
#[cfg(test)]
mod tests;

static HTTP_PORT_KEY: &str = "HTTP_PORT";
static DURABILITY_ENGINE_KEY: &str = "DURABILITY_ENGINE";
static LOG_LEVEL_KEY: &str = "LOG_LEVEL";

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let log_level = env_var_or_default(LOG_LEVEL_KEY, "info".to_owned());
    env_logger::init_from_env(env_logger::Env::new().default_filter_or(log_level));

    let port = require_env_var(HTTP_PORT_KEY)
        .unwrap()
        .parse::<u16>()
        .expect(&format!("Invalid {HTTP_PORT_KEY}."));

    let engine = DurabilityEngine::from_str(&require_env_var(DURABILITY_ENGINE_KEY).unwrap())
        .expect(&format!("Invalid {DURABILITY_ENGINE_KEY}."));
    let store = initialize_message_store(engine)
        .await
        .expect("Failed to initial message store.");
    let locked_store = Arc::new(RwLock::new(store));
    periodically_clean_up_store(locked_store.clone());

    let app_data = web::Data::new(locked_store);
    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(app_data.clone())
            .service(receive_message)
            .service(add_message)
            .service(acknowledge_message)
            .service(get_health)
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}

fn periodically_clean_up_store(message_store: Arc<RwLock<MessageStore>>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep_until(Instant::now().add(Duration::from_secs(60))).await;
            if let Err(error) = message_store.read().await.remove_expired().await {
                warn!("Failed to remove expired messages. {}", error);
            }
        }
    });
}

async fn initialize_message_store(
    engine: DurabilityEngine,
) -> Result<MessageStore, MessageStoreError> {
    let store = match engine {
        DurabilityEngine::Memory => MessageStore::new(),
        DurabilityEngine::Postgres => {
            MessageStore::new_with_durable_store(Box::new(PostgresMessageStore::create().await?))
        }
    };
    store.initialize().await?;
    Ok(store)
}
