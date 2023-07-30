use std::sync::Once;

use actix_http::Request;
use actix_web::http::Method;
use actix_web::web::ServiceConfig;
use actix_web::{test, web};
use chrono::{DateTime, Utc};
use serde_json::json;

use crate::message_store::MessageStore;
use crate::router::{acknowledge_message, add_message, get_health, receive_message};

static LOGGER_ONCE: Once = Once::new();

fn create_actix_app_configurer(message_store: MessageStore) -> impl Fn(&mut ServiceConfig) {
    LOGGER_ONCE.call_once(|| {
        env_logger::init_from_env(env_logger::Env::new().default_filter_or("debug"));
    });

    let app_data = web::Data::new(message_store);
    move |config: &mut ServiceConfig| {
        config
            .app_data(app_data.clone())
            .service(receive_message)
            .service(add_message)
            .service(acknowledge_message)
            .service(get_health);
    }
}

fn receive_message_request(queue_name: &str, subscriber_id: &str) -> Request {
    test::TestRequest::default()
        .method(Method::GET)
        .uri(&format!(
            "/queues/{queue_name}/messages?subscriberId={subscriber_id}"
        ))
        .to_request()
}

fn add_message_request(queue_name: &str, value: &str, expiry: &DateTime<Utc>) -> Request {
    test::TestRequest::default()
        .method(Method::POST)
        .uri(&format!("/queues/{queue_name}/messages"))
        .set_json(json!({
            "value": value,
            "expiry": expiry.timestamp()
        }))
        .to_request()
}

fn acknowledge_message_request(queue_name: &str, message_id: &str, subscriber_id: &str) -> Request {
    test::TestRequest::default()
        .method(Method::DELETE)
        .uri(&format!(
            "/queues/{queue_name}/messages/{message_id}?subscriberId={subscriber_id}"
        ))
        .to_request()
}

#[actix_rt::test]
async fn test_add_message() -> Result<(), anyhow::Error> {
    let app =
        init_service(App::new().configure(create_actix_app_configurer(MessageStore::new()))).await;

    let queue_name = "test_add_queue";
    let value = format!("{queue_name}_value");
    let expiry = Utc::now().add(Duration::days(1));

    let response = call_service(&app, add_message_request(queue_name, &value, &expiry)).await;
    assert!(response.status().is_success());

    let message: Message = serde_json::from_str(from_utf8(
        &to_bytes(response.into_body())
            .await
            .map_err(|_| anyhow!("Failed to parse response body."))?,
    )?)?;
    assert_eq!(message.expiry.timestamp(), expiry.timestamp());
    assert_eq!(message.value, value);

    Ok(())
}

#[actix_rt::test]
async fn test_empty_queue() -> Result<(), anyhow::Error> {
    let app =
        init_service(App::new().configure(create_actix_app_configurer(MessageStore::new()))).await;
    assert_eq!(
        StatusCode::NOT_FOUND,
        call_service(
            &app,
            receive_message_request("test_empty_queue", "test_empty_queue_subscriber"),
        )
        .await
        .status()
    );

    Ok(())
}

#[actix_rt::test]
async fn test_expired_message() -> Result<(), anyhow::Error> {
    let app =
        init_service(App::new().configure(create_actix_app_configurer(MessageStore::new()))).await;

    let queue_name = "test_expired_queue";
    assert!(call_service(
        &app,
        add_message_request(queue_name, "value", &Utc::now().sub(Duration::days(1))),
    )
    .await
    .status()
    .is_success());

    assert_eq!(
        StatusCode::NOT_FOUND,
        call_service(
            &app,
            receive_message_request(queue_name, &format!("{queue_name}_subscriber")),
        )
        .await
        .status()
    );

    Ok(())
}

#[actix_rt::test]
async fn test_receive_message() {
    let app =
        init_service(App::new().configure(create_actix_app_configurer(MessageStore::new()))).await;

    let queue_name = "test_receive_queue";
    let value = format!("{queue_name}_value");

    assert!(call_service(
        &app,
        add_message_request(queue_name, &value, &Utc::now().add(Duration::days(1))),
    )
    .await
    .status()
    .is_success());

    let response = call_service(
        &app,
        receive_message_request(queue_name, &format!("{queue_name}_subscriber")),
    )
    .await;
    assert!(response.status().is_success());

    let messages: Vec<Message> =
        serde_json::from_str(from_utf8(&to_bytes(response.into_body()).await.unwrap()).unwrap())
            .unwrap();
    let message = messages.iter().next().unwrap();
    assert_eq!(message.value, value);
}

#[actix_rt::test]
async fn test_acknowledge_message() {
    let app =
        init_service(App::new().configure(create_actix_app_configurer(MessageStore::new()))).await;

    let queue_name = "test_acknowledge_queue";
    let value = format!("{queue_name}_value");
    let subscriber_id = format!("{queue_name}_subscriber");

    let add_response = call_service(
        &app,
        add_message_request(queue_name, &value, &Utc::now().add(Duration::days(1))),
    )
    .await;
    assert!(add_response.status().is_success());
    let message: Message = serde_json::from_str(
        from_utf8(&to_bytes(add_response.into_body()).await.unwrap()).unwrap(),
    )
    .unwrap();

    assert!(call_service(
        &app,
        acknowledge_message_request(queue_name, &message.id, &subscriber_id),
    )
    .await
    .status()
    .is_success());

    assert_eq!(
        call_service(
            &app,
            receive_message_request(queue_name, &format!("{queue_name}_subscriber")),
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );

    let unique_subscriber_receive_response = call_service(
        &app,
        receive_message_request(queue_name, &format!("{queue_name}_different_subscriber")),
    )
    .await;
    assert!(unique_subscriber_receive_response.status().is_success());

    assert_eq!(
        serde_json::from_str::<Vec<Message>>(
            from_utf8(
                &to_bytes(unique_subscriber_receive_response.into_body())
                    .await
                    .unwrap()
            )
            .unwrap()
        )
        .unwrap()
        .iter()
        .next()
        .unwrap()
        .id,
        message.id
    );
}
