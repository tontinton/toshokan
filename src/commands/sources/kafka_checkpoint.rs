use std::collections::BTreeMap;

use async_trait::async_trait;
use color_eyre::Result;
use sqlx::PgPool;

use super::CheckpointCommiter;

#[derive(Debug, Clone)]
pub struct KafkaCheckpoint {
    source_id: String,
    pool: PgPool,
}

#[derive(Debug)]
pub struct KafkaCheckpointCommiter {
    checkpoint: KafkaCheckpoint,
    partitions_and_offsets: Vec<(i32, i64)>,
}

impl KafkaCheckpoint {
    pub fn new(source_id: String, pool: PgPool) -> Self {
        Self { source_id, pool }
    }

    pub async fn load(&self, partitions: &[i32]) -> Result<Vec<(i32, Option<i64>)>> {
        if partitions.is_empty() {
            return Ok(Vec::new());
        }

        let placeholders = (0..partitions.len())
            .map(|i| format!("${}", i + 2))
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "SELECT partition, offset_value FROM kafka_checkpoints WHERE source_id = $1 AND partition IN ({})",
            placeholders
        );

        let mut query = sqlx::query_as(&sql).bind(&self.source_id);
        for partition in partitions {
            query = query.bind(partition);
        }

        let partitions_and_offsets: Vec<(i32, i64)> = query.fetch_all(&self.pool).await?;
        debug!("Loaded checkpoints: {partitions_and_offsets:?}");

        let partitions_to_offsets = partitions_and_offsets
            .into_iter()
            .collect::<BTreeMap<i32, i64>>();

        Ok(partitions
            .iter()
            .map(|partition| (*partition, partitions_to_offsets.get(partition).copied()))
            .collect())
    }

    pub async fn save(&self, partitions_and_offsets: &[(i32, i64)]) -> Result<()> {
        let items = partitions_and_offsets
            .iter()
            .map(|(p, o)| (&self.source_id, *p, *o))
            .collect::<Vec<_>>();

        let mut sql = String::from(
            "INSERT INTO kafka_checkpoints (source_id, partition, offset_value) VALUES ",
        );

        let params = (0..items.len())
            .map(|i| format!("(${}, ${}, ${})", i * 3 + 1, i * 3 + 2, i * 3 + 3))
            .collect::<Vec<_>>();
        sql.push_str(&params.join(", "));
        sql.push_str(" ON CONFLICT (source_id, partition) DO UPDATE SET offset_value = EXCLUDED.offset_value");

        debug!("Saving checkpoints: {items:?}");

        let mut query = sqlx::query(&sql);
        for (source_id, partition, offset) in items {
            query = query.bind(source_id).bind(partition).bind(offset);
        }

        query.execute(&self.pool).await?;

        Ok(())
    }

    pub fn commiter(self, partitions_and_offsets: Vec<(i32, i64)>) -> KafkaCheckpointCommiter {
        KafkaCheckpointCommiter::new(self, partitions_and_offsets)
    }
}

impl KafkaCheckpointCommiter {
    pub fn new(checkpoint: KafkaCheckpoint, partitions_and_offsets: Vec<(i32, i64)>) -> Self {
        Self {
            checkpoint,
            partitions_and_offsets,
        }
    }
}

#[async_trait]
impl CheckpointCommiter for KafkaCheckpointCommiter {
    async fn commit(&self) -> Result<()> {
        self.checkpoint.save(&self.partitions_and_offsets).await?;
        Ok(())
    }
}
