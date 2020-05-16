//!
use actix_web::client::Client;
use actix_web::{error, middleware, web, App, Error, FromRequest, HttpResponse, HttpServer};
use anyhow::Result;
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use env_logger::Env;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// #[macro_use]
// extern crate log;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Document {
    id: String,
    val: u32,
    date: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Meta {
    received: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ElasticDocument {
    entity: Document,
    meta: Meta,
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            received: Utc::now(),
        }
    }
}

const MAX_SIZE: usize = 1024 * 1024;

async fn from_parquet(mut payload: web::Payload) -> Result<HttpResponse, Error> {
    let mut body = BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk?;
        if (body.len() + chunk.len()) > MAX_SIZE {
            return Err(error::ErrorBadRequest("overflow"));
        }
        body.extend_from_slice(&chunk);
    }

    todo!("Implement deserializing from parquet");
    // Ok(HttpResponse::Ok().finish())
}

async fn from_json(docs: web::Json<Vec<Value>>) -> Result<HttpResponse, Error> {
    let mut out_vec = Vec::new();

    for doc in docs.clone().into_iter() {
        let json_doc: Document = serde_json::from_value(doc)?;
        let out = ElasticDocument {
            entity: json_doc,
            meta: Meta {
                received: Utc::now(),
            },
        };
        out_vec.push(out);
    }

    println!("Processing {} documents.", out_vec.len());

    let mut payload = String::new();

    for e in out_vec {
        let json = serde_json::to_string(&e).unwrap();

        payload.push_str("{\"create\": {\"_index\": \"test\"}}\n");
        payload.push_str(&format!("{}\n", json));
    }

    let client = Client::default();
    let response = client
        .post("http://localhost:9200/test/_bulk")
        .header("Content-Type", "application/x-ndjson")
        .send_body(payload)
        .await?;

    println!("Response {:?}", response);

    Ok(HttpResponse::Ok().finish())
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    env_logger::from_env(Env::default().default_filter_or("info")).init();

    HttpServer::new(|| {
        App::new()
            .wrap(middleware::Logger::default())
            .service(
                web::resource("/from_json")
                    .app_data(web::Json::<Vec<Value>>::configure(|cfg| {
                        cfg.limit(MAX_SIZE)
                    }))
                    .route(web::post().to(from_json)),
            )
            .route("/from_parquet", web::post().to(from_parquet))
    })
    .bind("127.0.0.1:8000")?
    .run()
    .await
}
