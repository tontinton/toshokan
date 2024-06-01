mod args;
mod bincode;
mod commands;
mod config;
mod merge_directory;
mod opendal_file_handle;
mod unified_index;

use std::time::Duration;

use args::Args;
use color_eyre::eyre::Result;
use commands::{
    create::run_create, drop::run_drop, index::run_index, merge::run_merge, search::run_search,
};
use dotenvy::dotenv;
use pretty_env_logger::formatted_timed_builder;
use sqlx::{postgres::PgPoolOptions, PgPool};
use tokio::runtime::Builder;

use crate::args::{parse_args, SubCommand};

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

const DEFAULT_DEBUG_LOG_LEVEL: &str = "toshokan=trace,opendal::services=info";
const DEFAULT_RELEASE_LOG_LEVEL: &str = "toshokan=info,opendal::services=info";

const MAX_DB_CONNECTIONS: u32 = 100;

async fn open_db_pool(url: &str) -> Result<PgPool> {
    Ok(PgPoolOptions::new()
        .max_connections(MAX_DB_CONNECTIONS)
        .connect(url)
        .await?)
}

async fn async_main(args: Args) -> Result<()> {
    let pool = open_db_pool(&args.db.unwrap_or_else(|| {
        std::env::var("DATABASE_URL")
            .expect("database url must be provided using either --db or DATABASE_URL env var")
    }))
    .await?;

    match args.subcmd {
        SubCommand::Create(create_args) => {
            run_create(create_args, pool).await?;
        }
        SubCommand::Drop(drop_args) => {
            run_drop(drop_args, pool).await?;
        }
        SubCommand::Index(index_args) => {
            run_index(index_args, pool).await?;
        }
        SubCommand::Merge(merge_args) => {
            run_merge(merge_args, pool).await?;
        }
        SubCommand::Search(search_args) => {
            run_search(search_args, pool).await?;
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    color_eyre::install()?;

    // Load vars inside .env into env vars, does nothing if the file does not exist.
    let _ = dotenv();

    let default_log_level = if cfg!(debug_assertions) {
        DEFAULT_DEBUG_LOG_LEVEL
    } else {
        DEFAULT_RELEASE_LOG_LEVEL
    };

    let mut log_builder = formatted_timed_builder();
    log_builder.parse_filters(
        &std::env::var("RUST_LOG").unwrap_or_else(|_| default_log_level.to_string()),
    );
    log_builder.try_init()?;

    let args = parse_args();

    let runtime = Builder::new_multi_thread()
        .thread_keep_alive(Duration::from_secs(20))
        .enable_all()
        .build()?;
    runtime.block_on(async_main(args))
}
