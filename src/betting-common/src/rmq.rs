use backon::{ExponentialBuilder, Retryable};
use lapin::{
    BasicProperties, Connection, ConnectionProperties, PublisherConfirm,
    options::*,
    types::{FieldTable, ShortString},
};
use serde::Serialize;

pub mod exchanges {
    pub const USER: &str = "user_topic";
    pub const WALLET: &str = "wallet_topic";
    pub const BETTING: &str = "betting_topic";
    pub const EVENT: &str = "event_topic";
    pub const NOTIFICATION: &str = "notification_topic";
}

pub async fn connect_rmq(url: &str) -> Result<lapin::Channel, lapin::Error> {
    let rmq_conn = { || async { Connection::connect(url, ConnectionProperties::default()).await } }
        .retry(ExponentialBuilder::default().with_max_times(4))
        .await?;

    let channel = rmq_conn.create_channel().await?;
    Ok(channel)
}

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
    channel
        .basic_publish(
            exchange.into(),
            routing_key.into(),
            BasicPublishOptions::default(),
            &payload,
            properties,
        )
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
