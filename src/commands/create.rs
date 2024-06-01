use color_eyre::Result;
use sqlx::{query, PgPool};

use crate::{args::CreateArgs, index_config::IndexConfig};

pub async fn run_create(args: CreateArgs, pool: PgPool) -> Result<()> {
    let config = IndexConfig::from_path(&args.config_path).await?;

    query("INSERT INTO indexes (name, config) VALUES ($1, $2)")
        .bind(&config.name)
        .bind(&serde_json::to_value(&config)?)
        .execute(&pool)
        .await?;

    info!("Created index: {}", &config.name);

    Ok(())
}
