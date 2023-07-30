use actix_web::http::header::ContentType;
use actix_web::{get, HttpResponse, Responder};
use serde::Serialize;
use serde_json::json;

#[derive(Serialize)]
struct Healthy {
    status: String,
}

#[get("/health")]
pub async fn get_health() -> impl Responder {
    HttpResponse::Ok()
        .content_type(ContentType::json())
        .json(json!({ "status": "pass" }))
}
