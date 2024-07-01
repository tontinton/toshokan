use std::sync::Arc;

use color_eyre::Result;
use futures::future::join_all;
use sqlx::{query, query_as, PgPool};

use crate::args::DropArgs;

use super::{get_index_path, get_operator};

pub async fn run_drop(args: DropArgs, pool: &PgPool) -> Result<()> {
    let base_path = get_index_path(&args.name, pool).await?;

    let file_names: Vec<(String,)> =
        query_as("SELECT file_name FROM index_files WHERE index_name=$1")
            .bind(&args.name)
            .fetch_all(pool)
            .await?;
    let file_names_len = file_names.len();

    query("DELETE FROM indexes WHERE name=$1")
        .bind(&args.name)
        .execute(pool)
        .await?;

    let op = Arc::new(get_operator(&base_path).await?);
    join_all(
        file_names
            .into_iter()
            .map(|(file_name,)| (file_name, op.clone()))
            .map(|(file_name, op)| async move {
                if let Err(e) = op.delete(&file_name).await {
                    warn!(
                        "Failed to delete index file '{file_name}': {e}.
Don't worry, this just means the file is leaked, but will never be read from again."
                    );
                }
            }),
    )
    .await;

    info!(
        "Dropped index: {} ({} number of index files)",
        &args.name, file_names_len
    );

    Ok(())
}
