use std::path::Path;

use color_eyre::Result;
use sqlx::{query, PgPool};
use tantivy::{
    directory::{DirectoryClone, MmapDirectory},
    indexer::NoMergePolicy,
    Index, IndexWriter,
};
use tokio::{
    fs::{create_dir, remove_file},
    task::spawn_blocking,
};

use crate::{args::MergeArgs, merge_directory::MergeDirectory};

use super::{get_index_config, open_unified_directories, write_unified_index};

const MIN_TANTIVY_MEMORY: usize = 15_000_000;

pub async fn run_merge(args: MergeArgs, pool: PgPool) -> Result<()> {
    let config = get_index_config(&args.name, &pool).await?;

    let (ids, directories): (Vec<_>, Vec<_>) = open_unified_directories(&config.path, &pool)
        .await?
        .into_iter()
        .map(|(id, dir)| (id, dir.box_clone()))
        .unzip();

    if directories.len() <= 1 {
        info!("Need at least 2 files in index directory to be able to merge");
        return Ok(());
    }

    let _ = create_dir(&args.merge_dir).await;
    let output_dir = MmapDirectory::open(&args.merge_dir)?;

    let index = Index::open(MergeDirectory::new(directories, output_dir.box_clone())?)?;
    let mut index_writer: IndexWriter = index.writer_with_num_threads(1, MIN_TANTIVY_MEMORY)?;
    index_writer.set_merge_policy(Box::new(NoMergePolicy));

    let segment_ids = index.searchable_segment_ids()?;
    if segment_ids.len() > 1 {
        info!("Merging {} segments", segment_ids.len());
        index_writer.merge(&segment_ids).await?;
    }

    spawn_blocking(move || index_writer.wait_merging_threads()).await??;

    write_unified_index(index, &args.merge_dir, &config.name, &config.path, &pool).await?;

    let delete_result = query("DELETE FROM index_files WHERE id = ANY($1)")
        .bind(&ids)
        .execute(&pool)
        .await;

    for id in ids {
        let _ = remove_file(
            Path::new(&config.path)
                .join(format!("{}.index", id))
                .to_str()
                .expect("failed to build index path"),
        )
        .await;
    }

    delete_result?;

    Ok(())
}
