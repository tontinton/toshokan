use async_trait::async_trait;
use color_eyre::Result;
use tokio::{
    fs::File,
    io::{stdin, AsyncBufReadExt, AsyncRead, BufReader},
};

use super::{JsonMap, Source};

type AsyncBufReader = BufReader<Box<dyn AsyncRead + Send + Sync + Unpin>>;

pub struct BufSource {
    reader: AsyncBufReader,
    line: String,
}

impl BufSource {
    pub async fn from_path(path: &str) -> std::io::Result<Self> {
        debug!("Reading from '{}'", path);
        Ok(Self::from_buf_reader(BufReader::new(Box::new(
            File::open(&path).await?,
        ))))
    }

    pub fn from_stdin() -> Self {
        debug!("Reading from stdin");
        Self::from_buf_reader(BufReader::new(Box::new(stdin())))
    }

    fn from_buf_reader(reader: AsyncBufReader) -> Self {
        Self {
            reader,
            line: String::new(),
        }
    }
}

#[async_trait]
impl Source for BufSource {
    async fn get_one(&mut self) -> Result<Option<JsonMap>> {
        let len = self.reader.read_line(&mut self.line).await?;
        if len == 0 {
            return Ok(None);
        }

        let map = serde_json::from_str(&self.line)?;
        self.line.clear();
        Ok(map)
    }
}
