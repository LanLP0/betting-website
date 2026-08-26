use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, middleware::Logger, web};
use betting_common::{connect_rmq, exchanges, http::BadRequestResponse, req_get_user_role};
use clickhouse::Client as ClickhouseClient;
use clickhouse::Row;
use futures_util::StreamExt;
use lapin::{options::*, types::FieldTable};
use serde::{Deserialize, Serialize};
use std::env;
use std::time::Duration;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

#[derive(Row, Serialize, Deserialize, Debug, ToSchema)]
struct EventMetric {
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<u64>,
    event_id: String,
    event_type: String,
    value1: String,
    value2: String,
    value3: String,
    payload: Option<String>,
    trace_id: Option<String>,
}

#[derive(Deserialize, ToSchema)]
struct MetricQuery {
    limit: Option<u64>,
    event_type: Option<String>,
    trace_id: Option<String>,
}

#[derive(OpenApi)]
#[openapi(
    paths(
        get_health,
        get_metrics,
    ),
    components(
        schemas(EventMetric, MetricQuery)
    ),
    tags(
        (name = "Management", description = "Management and System Metrics APIs")
    )
)]
struct ApiDoc;

#[allow(dead_code)]
struct AppState {
    clickhouse: ClickhouseClient,
}

fn validate_input_event_query(query: &str) -> bool {
    !query.is_empty()
        && query
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
}

#[utoipa::path(
    get,
    path = "/api/v1/management/health",
    responses(
        (status = 200, description = "Service Healthy")
    )
)]
async fn get_health() -> impl Responder {
    HttpResponse::Ok().body("Ok")
}

/*
 * ARCHITECTURAL NOTICE: ANALYTICAL METRICS QUERYING & OLAP OFF-LOADING
 *
 * Context & Scenario:
 * ClickHouse serves as the dedicated column-oriented OLAP warehouse, storing historical telemetry,
 * audit traces, and event logs fanned out from RabbitMQ topic exchanges.
 *
 * Performance & Architecture Blueprint:
 * 1. Read Path Isolation:
 *    - Analytical metric queries (`get_metrics`) query ClickHouse directly and never touch
 *      PostgreSQL, completely isolating high-frequency telemetry from transactional financial tables.
 * 2. Columnar Indexing:
 *    - Queries filter by `event_type` and `trace_id` leveraging the MergeTree primary key index
 *      `(timestamp, event_id, event_type)`.
 */
#[utoipa::path(
    get,
    path = "/api/v1/management/metrics",
    params(
        ("limit" = Option<u64>, Query, description = "Maximum number of metrics to return (default 100, max 1000)"),
        ("event_type" = Option<String>, Query, description = "Filter by RabbitMQ routing key / event type"),
        ("trace_id" = Option<String>, Query, description = "Filter by distributed trace / correlation ID")
    ),
    responses(
        (status = 200, description = "ClickHouse metrics", body = Vec<EventMetric>),
        (status = 400, description = "Invalid query parameters"),
        (status = 403, description = "Forbidden - Admin access required"),
        (status = 500, description = "Internal Server Error")
    )
)]
async fn get_metrics(
    req: HttpRequest,
    query: web::Query<MetricQuery>,
    data: web::Data<AppState>,
) -> impl Responder {
    let auth_user_role = req_get_user_role(&req);
    if auth_user_role != "admin" {
        return HttpResponse::Forbidden().finish();
    }

    let limit = query.limit.unwrap_or(100).clamp(1, 1000);

    let mut ch_query = "SELECT timestamp, event_id, event_type, toString(value1) AS value1, toString(value2) AS value2, toString(value3) AS value3, payload, trace_id FROM metrics_schema.events_log WHERE 1=1".to_string();

    if let Some(ref et) = query.event_type {
        if !validate_input_event_query(et) {
            return HttpResponse::BadRequest().json(BadRequestResponse {
                status: "failed".into(),
                err_code: "invalid_params".into(),
                should_retry: false,
                msg: Some("Invalid event type format. Allowed: alphanumeric, _, -".into()),
            });
        }

        ch_query.push_str(&format!(" AND event_type = '{}'", et));
    }

    if let Some(ref tr) = query.trace_id {
        if !validate_input_event_query(tr) {
            return HttpResponse::BadRequest().json(BadRequestResponse {
                status: "failed".into(),
                err_code: "invalid_params".into(),
                should_retry: false,
                msg: Some("Invalid trace ID format. Allowed: alphanumeric, _, -".into()),
            });
        }

        ch_query.push_str(&format!(" AND trace_id = '{}'", tr.replace('\'', "''")));
    }

    ch_query.push_str(&format!(" ORDER BY timestamp DESC LIMIT {}", limit));

    let mut cursor = match data.clickhouse.query(&ch_query).fetch::<EventMetric>() {
        Ok(c) => c,
        Err(e) => {
            log::error!("Failed to fetch metrics from ClickHouse: {:?}", e);
            return HttpResponse::InternalServerError().finish();
        }
    };

    let mut metrics = Vec::new();
    while let Ok(Some(row)) = cursor.next().await {
        metrics.push(row);
    }

    HttpResponse::Ok().json(metrics)
}

/*
 * ARCHITECTURAL NOTICE: CLICKHOUSE HIGH-THROUGHPUT METRIC INGESTION & BATCHING
 *
 * Context & Scenario:
 * Consumes `#` wildcard topics across all internal domain exchanges:
 * `user_topic`, `wallet_topic`, `betting_topic`, `event_topic`, `notification_topic`.
 *
 * ClickHouse Performance Guard:
 * Direct single-row inserts per RabbitMQ message will generate thousands of tiny data parts
 * ("Too many parts in all data parts in table" error in ClickHouse MergeTree).
 *
 * Target Batching Strategy:
 * Messages are buffered and flushed in batches (up to 100 rows or every 500ms timeout)
 * to maintain high write throughput and low I/O overhead.
 */
async fn metrics_rmq_consumer(rmq_chan: lapin::Channel, clickhouse: ClickhouseClient) {
    let q = rmq_chan
        .queue_declare(
            "metrics_clickhouse_queue".into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await;

    if let Ok(q) = q {
        let topic_exchanges = vec![
            exchanges::USER,
            exchanges::WALLET,
            exchanges::BETTING,
            exchanges::EVENT,
            exchanges::NOTIFICATION,
        ];

        for ex in topic_exchanges {
            let _ = rmq_chan
                .queue_bind(
                    q.name().to_owned(),
                    ex.into(),
                    "#".into(),
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await;
        }

        if let Ok(mut consumer) = rmq_chan
            .basic_consume(
                q.name().to_owned(),
                "metrics_consumer".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
        {
            let mut batch_buffer: Vec<(EventMetric, lapin::message::Delivery)> =
                Vec::with_capacity(100);
            let mut flush_interval = tokio::time::interval(Duration::from_millis(500));

            loop {
                tokio::select! {
                    delivery_opt = consumer.next() => {
                        match delivery_opt {
                            Some(Ok(delivery)) => {
                                let routing_key = delivery.routing_key.as_str().to_string();
                                let payload_str = String::from_utf8_lossy(&delivery.data).to_string();
                                let event_id = Uuid::new_v4().to_string();

                                // Extract trace_id from headers or correlation_id
                                let trace_id = delivery
                                    .properties
                                    .headers()
                                    .as_ref()
                                    .and_then(|h| h.inner().get("x-trace-id"))
                                    .and_then(|v| match v {
                                        lapin::types::AMQPValue::LongString(s) => Some(s.to_string()),
                                        lapin::types::AMQPValue::ShortString(s) => Some(s.to_string()),
                                        _ => None,
                                    })
                                    .or_else(|| {
                                        delivery
                                            .properties
                                            .correlation_id()
                                            .as_ref()
                                            .map(|c| c.to_string())
                                    });

                                let mut val1 = String::new();
                                let mut val2 = String::new();
                                let mut val3 = String::new();

                                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&payload_str) {
                                    if let Some(v) = json.get("user_id").or_else(|| json.get("id")) {
                                        val1 = v.to_string().trim_matches('"').to_string();
                                    }
                                    if let Some(v) = json.get("amount").or_else(|| json.get("status")) {
                                        val2 = v.to_string().trim_matches('"').to_string();
                                    }
                                    if let Some(v) = json.get("bet_id").or_else(|| json.get("event_id")) {
                                        val3 = v.to_string().trim_matches('"').to_string();
                                    }
                                }

                                let row = EventMetric {
                                    timestamp: None,
                                    event_id,
                                    event_type: routing_key,
                                    value1: val1,
                                    value2: val2,
                                    value3: val3,
                                    payload: Some(payload_str),
                                    trace_id,
                                };

                                batch_buffer.push((row, delivery));

                                if batch_buffer.len() >= 100 {
                                    flush_metric_batch(&clickhouse, &mut batch_buffer).await;
                                }
                            }
                            Some(Err(e)) => {
                                log::error!("RabbitMQ delivery stream error in metrics consumer: {:?}", e);
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                            None => {
                                log::warn!("Metrics consumer stream terminated.");
                                break;
                            }
                        }
                    }
                    _ = flush_interval.tick() => {
                        if !batch_buffer.is_empty() {
                            flush_metric_batch(&clickhouse, &mut batch_buffer).await;
                        }
                    }
                }
            }
        }
    }
}

/// Batch flush metrics buffer to ClickHouse and ACK deliveries
async fn flush_metric_batch(
    clickhouse: &ClickhouseClient,
    batch: &mut Vec<(EventMetric, lapin::message::Delivery)>,
) {
    if batch.is_empty() {
        return;
    }

    let mut inserter = match clickhouse
        .insert::<EventMetric>("metrics_schema.events_log")
        .await
    {
        Ok(ins) => ins,
        Err(e) => {
            log::error!("Failed to initialize ClickHouse inserter: {:?}", e);
            // ACK to prevent head-of-line blocking on metric logs
            for (_, delivery) in batch.drain(..) {
                let _ = delivery.ack(BasicAckOptions::default()).await;
            }
            return;
        }
    };

    for (metric, _) in batch.iter() {
        let _ = inserter.write(metric).await;
    }

    if let Err(e) = inserter.end().await {
        log::error!("Failed to commit batch insert to ClickHouse: {:?}", e);
    }

    for (_, delivery) in batch.drain(..) {
        let _ = delivery.ack(BasicAckOptions::default()).await;
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    // let db_url = env::var("DATABASE_URL").expect("DATABASE_URL required");
    // let pool = connect_pg(&db_url, 5).await.expect("Failed DB connection");

    let rmq_url = env::var("RABBITMQ_URL").expect("RABBITMQ_URL required");
    let rmq_chan = connect_rmq(&rmq_url, "management-service")
        .await
        .expect("Failed RMQ connection");

    let clickhouse_url =
        env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".into());
    let clickhouse = ClickhouseClient::default()
        .with_url(clickhouse_url)
        .with_user("default")
        .with_password("");

    tokio::spawn(metrics_rmq_consumer(rmq_chan.clone(), clickhouse.clone()));

    let state = web::Data::new(AppState { clickhouse });

    let openapi = ApiDoc::openapi();

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(state.clone())
            .service(
                SwaggerUi::new("/api/v1/swagger/{_:.*}")
                    .url("/api-docs/openapi.json", openapi.clone()),
            )
            .route("/api/v1/management/health", web::get().to(get_health))
            .route("/api/v1/management/metrics", web::get().to(get_metrics))
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
