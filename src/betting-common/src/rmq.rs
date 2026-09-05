use backon::{ExponentialBuilder, Retryable};
use lapin::{
    BasicProperties, Connection, ConnectionProperties, PublisherConfirm, options::*,
    types::{AMQPValue, FieldTable},
};
use serde::Serialize;
use std::time::Duration;

pub mod exchanges {
    pub const USER: &str = "user_topic";
    pub const WALLET: &str = "wallet_topic";
    pub const BETTING: &str = "betting_topic";
    pub const EVENT: &str = "event_topic";
    pub const NOTIFICATION: &str = "notification_topic";
    pub const DLX: &str = "dlx_topic";
    pub const DEAD_LETTER_QUEUE: &str = "dead_letter_queue";
}

pub async fn connect_rmq(url: &str, con_name: &str) -> Result<lapin::Channel, lapin::Error> {
    let props = ConnectionProperties::default()
        .with_connection_name(con_name.into())
        .enable_auto_recover()
        .configure_backoff(|b| {
            b.with_jitter()
                .with_max_times(3)
                .with_min_delay(Duration::from_secs(1))
                .with_factor(3.0)
        });

    let rmq_conn = Connection::connect(url, props).await?; // Has internal retry mechanics

    let channel = rmq_conn.create_channel().await?;
    Ok(channel)
}

/// Setup centralized Dead-Letter Exchange and Dead-Letter Queue
pub async fn setup_dlq(channel: &lapin::Channel) -> Result<(), lapin::Error> {
    // 1. Declare Topic DLX
    channel
        .exchange_declare(
            exchanges::DLX.into(),
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    // 2. Declare durable Dead-Letter Queue
    let dlq = channel
        .queue_declare(
            exchanges::DEAD_LETTER_QUEUE.into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    // 3. Bind DLQ to DLX with wildcard '#' to capture all unhandled / poisoned messages
    channel
        .queue_bind(
            dlq.name().to_owned(),
            exchanges::DLX.into(),
            "#".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;

    Ok(())
}

/// Declare a durable queue configured with Dead-Letter Exchange (DLX) routing arguments
pub async fn declare_queue_with_dlx(
    channel: &lapin::Channel,
    queue_name: &str,
    dlx_routing_key: &str,
) -> Result<lapin::Queue, lapin::Error> {
    let mut args = FieldTable::default();
    args.insert(
        "x-dead-letter-exchange".into(),
        AMQPValue::LongString(exchanges::DLX.into()),
    );
    args.insert(
        "x-dead-letter-routing-key".into(),
        AMQPValue::LongString(dlx_routing_key.into()),
    );

    channel
        .queue_declare(
            queue_name.into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            args,
        )
        .await
}

/// Publish an event to a RMQ `channel` with retry mechanics
pub async fn publish_event_props(
    channel: &lapin::Channel,
    exchange: &str,
    routing_key: &str,
    payload: impl Serialize,
    properties: BasicProperties,
) -> Result<PublisherConfirm, lapin::Error> {
    let payload = serde_json::to_vec(&payload).map_err(|e| {
        lapin::ErrorKind::IOError(std::sync::Arc::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            e.to_string(),
        )))
    })?;
    {
        || async {
            channel
                .basic_publish(
                    exchange.into(),
                    routing_key.into(),
                    BasicPublishOptions::default(),
                    &payload,
                    properties.clone(),
                )
                .await
        }
    }
    .retry(ExponentialBuilder::default().with_jitter())
    .when(crate::lapin_retry_when)
    .await
}

pub async fn publish_event(
    channel: &lapin::Channel,
    exchange: &str,
    routing_key: &str,
    payload: impl Serialize,
) -> Result<PublisherConfirm, lapin::Error> {
    publish_event_props(
        channel,
        exchange,
        routing_key,
        payload,
        BasicProperties::default(),
    )
    .await
}

pub async fn publish_event_with_trace(
    channel: &lapin::Channel,
    exchange: &str,
    routing_key: &str,
    payload: impl Serialize,
    trace_id: &str,
) -> Result<PublisherConfirm, lapin::Error> {
    let mut headers = FieldTable::default();
    headers.insert(
        "x-trace-id".into(),
        lapin::types::AMQPValue::LongString(trace_id.to_string().into()),
    );
    let props = BasicProperties::default()
        .with_correlation_id(trace_id.to_string().into())
        .with_headers(headers);
    publish_event_props(channel, exchange, routing_key, payload, props).await
}
