use std::path::Path;

use color_eyre::{eyre::eyre, Result};
use sqlx::{query, query_as, PgPool};
use tokio::fs::remove_file;

use crate::args::DropArgs;

use super::get_index_path;

pub async fn run_drop(args: DropArgs, pool: &PgPool) -> Result<()> {
    let base_path = get_index_path(&args.name, pool).await?;

    let file_names: Vec<(String,)> =
        query_as("SELECT file_name FROM index_files WHERE index_name=$1")
            .bind(&args.name)
            .fetch_all(pool)
            .await?;
    let file_names_len = file_names.len();

    for (file_name,) in file_names {
        let _ = remove_file(
            Path::new(&base_path)
                .join(file_name)
                .to_str()
                .ok_or_else(|| eyre!("failed to build index file path"))?,
        )
        .await;
    }

    query("DELETE FROM indexes WHERE name=$1")
        .bind(&args.name)
        .execute(pool)
        .await?;

    info!(
        "Dropped index: {} ({} number of index files)",
        &args.name, file_names_len
    );

    Ok(())
}
