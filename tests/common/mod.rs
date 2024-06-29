use std::path::Path;

use aws_sdk_s3::{
    config::{Credentials, Region, SharedCredentialsProvider},
    Client,
};
use clap::Parser;
use color_eyre::Result;
use pretty_env_logger::formatted_timed_builder;
use sqlx::{migrate::Migrator, postgres::PgPoolOptions, PgPool};
use testcontainers::{runners::AsyncRunner, ContainerAsync, RunnableImage};
use testcontainers_modules::{localstack::LocalStack, postgres::Postgres};
use tokio::{fs::read_dir, sync::mpsc};
use toshokan::{args::SearchArgs, commands::search::run_search_with_callback};

pub const AWS_ACCESS_KEY_ID: &str = "test1";
pub const AWS_SECRET_ACCESS_KEY: &str = "test2";
pub const AWS_REGION: &str = "us-east-1";

const MAX_DB_CONNECTIONS: u32 = 100;

static MIGRATOR: Migrator = sqlx::migrate!();

#[allow(dead_code)]
pub struct PostgresContainer {
    /// Keep container alive (container is deleted on drop).
    _container: ContainerAsync<Postgres>,

    /// The underlying sqlx connection to the postgres inside the container.
    pub pool: PgPool,
}

#[allow(dead_code)]
pub struct S3Container {
    /// Keep container alive (container is deleted on drop).
    _container: ContainerAsync<LocalStack>,

    /// The s3 endpoint url to connect to the s3 instance inside the container.
    pub endpoint_url: String,

    /// The s3 sdk client.
    pub client: Client,
}

async fn open_db_pool(url: &str) -> Result<PgPool> {
    Ok(PgPoolOptions::new()
        .max_connections(MAX_DB_CONNECTIONS)
        .connect(url)
        .await?)
}

#[allow(dead_code)]
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

#[allow(dead_code)]
pub async fn run_localstack(services: &str) -> Result<ContainerAsync<LocalStack>> {
    let image: RunnableImage<LocalStack> = LocalStack::default().into();
    let image = image.with_env_var(("SERVICES", services));
    let container = image.start().await?;
    Ok(container)
}

#[allow(dead_code)]
pub async fn run_s3() -> Result<S3Container> {
    let container = run_localstack("s3").await?;
    let s3_port = container.get_host_port_ipv4(4566).await?;
    let endpoint_url = format!("http://127.0.0.1:{s3_port}");

    let client = Client::from_conf(
        aws_sdk_s3::Config::builder()
            .region(Region::new(AWS_REGION))
            .endpoint_url(&endpoint_url)
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                AWS_ACCESS_KEY_ID,
                AWS_SECRET_ACCESS_KEY,
                None,
                None,
                "local",
            )))
            .build(),
    );

    Ok(S3Container {
        _container: container,
        endpoint_url,
        client,
    })
}

#[allow(dead_code)]
pub async fn get_number_of_files_in_dir<P: AsRef<Path>>(dir: P) -> std::io::Result<usize> {
    let mut file_count = 0;
    let mut entries = read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file() {
            file_count += 1;
        }
    }
    Ok(file_count)
}

#[allow(dead_code)]
pub async fn search_one(index: &str, query: &str, pool: &PgPool) -> Result<String> {
    let (tx, mut rx) = mpsc::channel(1);
    run_search_with_callback(
        SearchArgs::parse_from(["", index, query, "--limit", "1"]),
        pool,
        Box::new(move |doc| {
            tx.try_send(doc).unwrap();
        }),
    )
    .await?;
    Ok(rx
        .recv()
        .await
        .expect("Search for '{query}' found nothing."))
}

#[allow(dead_code)]
pub fn test_init() {
    color_eyre::install().unwrap();

    let mut log_builder = formatted_timed_builder();
    log_builder.parse_filters("toshokan=trace,opendal::services=info");
    log_builder.try_init().unwrap();
}
