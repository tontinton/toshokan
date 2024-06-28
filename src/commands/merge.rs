use std::{path::Path, sync::Arc};

use color_eyre::Result;
use futures::future::join_all;
use sqlx::{query, PgPool};
use tantivy::{
    directory::{DirectoryClone, MmapDirectory},
    indexer::NoMergePolicy,
    Index, IndexWriter,
};
use tokio::{fs::create_dir_all, task::spawn_blocking};

use crate::{args::MergeArgs, merge_directory::MergeDirectory};

use super::{get_index_config, get_operator, open_unified_directories, write_unified_index};

const MIN_TANTIVY_MEMORY: usize = 15_000_000;

pub async fn run_merge(args: MergeArgs, pool: &PgPool) -> Result<()> {
    let config = get_index_config(&args.name, pool).await?;

    let (ids, directories): (Vec<_>, Vec<_>) = open_unified_directories(&config.path, pool)
        .await?
        .into_iter()
        .map(|(id, dir)| (id, dir.box_clone()))
        .unzip();

    if directories.len() <= 1 {
        info!("Need at least 2 files in index directory to be able to merge");
        return Ok(());
    }

    let id = uuid::Uuid::now_v7().hyphenated().to_string();
    let index_dir = Path::new(&args.merge_dir).join(&id);
    let _ = create_dir_all(&index_dir).await;
    let output_dir = MmapDirectory::open(&index_dir)?;

    let index = Index::open(MergeDirectory::new(directories, output_dir.box_clone())?)?;
    let mut index_writer: IndexWriter = index.writer_with_num_threads(1, MIN_TANTIVY_MEMORY)?;
    index_writer.set_merge_policy(Box::new(NoMergePolicy));

    let segment_ids = index.searchable_segment_ids()?;
    if segment_ids.len() > 1 {
        info!("Merging {} segments", segment_ids.len());
        index_writer.merge(&segment_ids).await?;
    }

    spawn_blocking(move || index_writer.wait_merging_threads()).await??;

    write_unified_index(&id, &index, &index_dir, &config.name, &config.path, pool).await?;

    query("DELETE FROM index_files WHERE id = ANY($1)")
        .bind(&ids)
        .execute(pool)
        .await?;

    let op = Arc::new(get_operator(&config.path).await?);
    join_all(
        ids.into_iter()
            .map(|id| (format!("{}.index", id), op.clone()))
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

    Ok(())
}
