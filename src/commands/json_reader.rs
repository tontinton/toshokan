use async_trait::async_trait;
use color_eyre::Result;
use tokio::{
    fs::File,
    io::{stdin, AsyncBufReadExt, AsyncRead, BufReader},
};

type JsonMap = serde_json::Map<String, serde_json::Value>;
type AsyncBufReader = BufReader<Box<dyn AsyncRead + Send + Sync + Unpin>>;

#[async_trait]
pub trait JsonReader {
    async fn next(&mut self) -> Result<Option<JsonMap>>;
}

struct BufJsonReader {
    reader: AsyncBufReader,
    line: String,
}

impl BufJsonReader {
    async fn from_path(path: &str) -> std::io::Result<Self> {
        debug!("Reading from '{}'", path);
        Ok(Self::from_buf_reader(BufReader::new(Box::new(
            File::open(&path).await?,
        ))))
    }

    fn from_stdin() -> Self {
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
impl JsonReader for BufJsonReader {
    async fn next(&mut self) -> Result<Option<JsonMap>> {
        let len = self.reader.read_line(&mut self.line).await?;
        if len == 0 {
            return Ok(None);
        }

        let map = serde_json::from_str(&self.line)?;
        self.line.clear();
        return Ok(map);
    }
}

pub async fn build_json_reader(input: Option<&str>) -> Result<Box<dyn JsonReader + Unpin>> {
    Ok(match input {
        Some(path) => Box::new(BufJsonReader::from_path(path).await?),
        None => Box::new(BufJsonReader::from_stdin()),
    })
}
