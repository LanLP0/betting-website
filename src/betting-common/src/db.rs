use backon::{ExponentialBuilder, Retryable};
use sqlx::{PgPool, postgres::PgPoolOptions};

/// Connect to Postgres database using `url` with `max_connections`
pub async fn connect_pg(url: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    let pool = {
        || async {
            PgPoolOptions::new()
                .max_connections(max_connections)
                .connect(url)
                .await
        }
    }
    .retry(
        ExponentialBuilder::default()
            .with_jitter()
            .with_max_times(4),
    )
    .await?;

    Ok(pool)
}
