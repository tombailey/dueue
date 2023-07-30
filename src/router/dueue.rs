use std::sync::Arc;

use actix_web::{delete, get, post, web, HttpResponse, Responder};
use chrono::serde::ts_seconds;
use chrono::{DateTime, Duration, Utc};
use log::error;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::RwLock;

use crate::message_store::model::Message;
use crate::message_store::MessageStore;

impl Into<HttpResponse> for Message {
    fn into(self) -> HttpResponse {
        HttpResponse::Ok().json(json!({
            "id": self.id,
            "value": self.value,
            "expiry": self.expiry.timestamp(),
        }))
    }
}

trait IntoHttpResponse {
    fn into(self) -> HttpResponse;
}

impl IntoHttpResponse for Option<Message> {
    fn into(self) -> HttpResponse {
        self.map(Into::into)
            .unwrap_or(HttpResponse::NotFound().finish())
    }
}

impl IntoHttpResponse for Vec<Message> {
    fn into(self) -> HttpResponse {
        HttpResponse::Ok().json(self)
    }
}

#[derive(Deserialize)]
pub struct Receiver {
    #[serde(rename(deserialize = "subscriberId"))]
    id: String,
    #[serde(rename(deserialize = "acknowledgementDuration"))]
    acknowledgement_duration: i64,
}

#[get("/queues/{name}/messages")]
pub async fn receive_message(
    (queue_name, receiver, store): (
        web::Path<String>,
        web::Query<Receiver>,
        web::Data<Arc<RwLock<MessageStore>>>,
    ),
) -> impl Responder {
    // TODO: allow get many?
    store
        .read()
        .await
        .next(
            &queue_name,
            &receiver.id,
            Duration::seconds(receiver.acknowledgement_duration),
        )
        .await
        .map(|maybe_message| {
            maybe_message.map_or(IntoHttpResponse::into(None), |message| {
                IntoHttpResponse::into(vec![message])
            })
        })
        .unwrap_or_else(|error| {
            error!("{error}");
            HttpResponse::InternalServerError().finish()
        })
}

#[derive(Serialize, Deserialize)]
pub struct AddMessagePayload {
    value: String,
    #[serde(with = "ts_seconds")]
    expiry: DateTime<Utc>,
}

#[post("/queues/{name}/messages")]
pub async fn add_message(
    (name, payload, store): (
        web::Path<String>,
        web::Json<AddMessagePayload>,
        web::Data<Arc<RwLock<MessageStore>>>,
    ),
) -> impl Responder {
    store
        .read()
        .await
        .add(&name, &payload.value, &payload.expiry)
        .await
        .map(Into::into)
        .unwrap_or_else(|error| {
            error!("{error}");
            HttpResponse::InternalServerError().finish()
        })
}

#[derive(Deserialize)]
pub struct Acknowledger {
    #[serde(rename(deserialize = "subscriberId"))]
    id: String,
}

#[delete("/queues/{queue_name}/messages/{message_id}")]
pub async fn acknowledge_message(
    (path, acknowledger, store): (
        web::Path<(String, String)>,
        web::Query<Acknowledger>,
        web::Data<Arc<RwLock<MessageStore>>>,
    ),
) -> impl Responder {
    store
        .read()
        .await
        .acknowledge(&path.0, &acknowledger.id, &path.1)
        .await
        .map(|_| HttpResponse::NoContent().finish())
        .unwrap_or_else(|error| {
            error!("{error}");
            HttpResponse::InternalServerError().finish()
        })
}
