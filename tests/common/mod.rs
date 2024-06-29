use color_eyre::Result;
use pretty_env_logger::formatted_timed_builder;
use sqlx::{migrate::Migrator, postgres::PgPoolOptions, PgPool};
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::postgres::Postgres;

static MIGRATOR: Migrator = sqlx::migrate!();

const MAX_DB_CONNECTIONS: u32 = 100;

pub struct PostgresContainer {
    /// Keep container alive (container is deleted on drop).
    _container: ContainerAsync<Postgres>,

    /// The underlying sqlx connection to the postgres inside the container.
    pub pool: PgPool,
}

async fn open_db_pool(url: &str) -> Result<PgPool> {
    Ok(PgPoolOptions::new()
        .max_connections(MAX_DB_CONNECTIONS)
        .connect(url)
        .await?)
}

pub async fn run_postgres() -> Result<PostgresContainer> {
    let container = Postgres::default().start().await?;
    let pool = open_db_pool(&format!(
        "postgres://postgres:postgres@127.0.0.1:{}/postgres",
        container.get_host_port_ipv4(5432).await?
    ))
    .await?;

    MIGRATOR.run(&pool).await?;

    Ok(PostgresContainer {
        _container: container,
        pool,
    })
}

pub fn test_init() {
    color_eyre::install().unwrap();

    let mut log_builder = formatted_timed_builder();
    log_builder.parse_filters("toshokan=trace,opendal::services=info");
    log_builder.try_init().unwrap();
}
