use std::{
    fs::canonicalize,
    path::{Path, PathBuf},
};

use color_eyre::Result;
use sqlx::{migrate::Migrator, postgres::PgPoolOptions, PgPool};
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::postgres::Postgres as PostgresContainer;

static MIGRATOR: Migrator = sqlx::migrate!();

const MAX_DB_CONNECTIONS: u32 = 100;

pub struct Postgres {
    /// Keep container alive (container is deleted on drop).
    _container: ContainerAsync<PostgresContainer>,

    /// The underlying sqlx connection to the postgres inside the container.
    pub pool: PgPool,
}

async fn open_db_pool(url: &str) -> Result<PgPool> {
    Ok(PgPoolOptions::new()
        .max_connections(MAX_DB_CONNECTIONS)
        .connect(url)
        .await?)
}

pub async fn run_postgres() -> Result<Postgres> {
    let container = PostgresContainer::default().start().await?;
    let pool = open_db_pool(&format!(
        "postgres://postgres:postgres@127.0.0.1:{}/postgres",
        container.get_host_port_ipv4(5432).await?
    ))
    .await?;

    MIGRATOR.run(&pool).await?;

    Ok(Postgres {
        _container: container,
        pool,
    })
}

pub fn get_test_file_path(test_file: &str) -> PathBuf {
    canonicalize(&Path::new(file!()))
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("test_files")
        .join(test_file)
}
