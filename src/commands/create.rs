use color_eyre::{eyre::bail, Result};
use sqlx::{query, PgPool};

use crate::{
    args::CreateArgs,
    config::{FieldType, IndexConfig},
};

pub async fn run_create(args: CreateArgs, pool: PgPool) -> Result<()> {
    let config = IndexConfig::from_path(&args.config_path).await?;

    let array_static_object_exists = config
        .schema
        .fields
        .iter()
        .any(|x| x.array && matches!(x.type_, FieldType::StaticObject(_)));
    if array_static_object_exists {
        bail!("array of static objects are currently unsupported");
    }

    query("INSERT INTO indexes (name, config) VALUES ($1, $2)")
        .bind(&config.name)
        .bind(&serde_json::to_value(&config)?)
        .execute(&pool)
        .await?;

    info!("Created index: {}", &config.name);

    Ok(())
}
