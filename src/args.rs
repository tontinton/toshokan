use std::time::Duration;

use clap::Parser;
use humantime::parse_duration;

fn parse_humantime(s: &str) -> Result<Duration, String> {
    parse_duration(s).map_err(|e| format!("Failed to parse duration: {}. Please refer to https://docs.rs/humantime/2.1.0/humantime/fn.parse_duration.html", e))
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[clap(
        long,
        help = "Postgres DB connection url.
Can also be provided by a DATABASE_URL env var, but only if this arg is not provided."
    )]
    pub db: Option<String>,

    #[clap(subcommand)]
    pub subcmd: SubCommand,
}

#[derive(Parser, Debug, Clone)]
pub enum SubCommand {
    #[clap(name = "create")]
    Create(CreateArgs),

    #[clap(name = "drop")]
    Drop(DropArgs),

    #[clap(name = "index")]
    Index(IndexArgs),

    #[clap(name = "merge")]
    Merge(MergeArgs),

    #[clap(name = "search")]
    Search(SearchArgs),
}

#[derive(Parser, Debug, Clone)]
pub struct CreateArgs {
    #[clap(help = "Path to the input config file.")]
    pub config_path: String,
}

#[derive(Parser, Debug, Clone)]
pub struct DropArgs {
    #[clap(help = "The index name.")]
    pub name: String,
}

#[derive(Parser, Debug, Clone)]
pub struct IndexArgs {
    #[clap(help = "The index name.")]
    pub name: String,

    #[clap(help = "Path to the input jsonl file you want to index.
Read from stdin by not providing any file path.")]
    pub input: Option<String>,

    #[clap(
        short,
        long,
        help = "Whether to stream from the source without terminating. Will stop only once the source is closed.",
        default_value = "false"
    )]
    pub stream: bool,

    #[clap(
        long,
        help = "How much time to collect docs from the source until an index file should be generated.
Only used when streaming.
Examples: '5s 500ms', '2m 10s'.",
        default_value = "30s",
        value_parser = parse_humantime
    )]
    pub commit_interval: Duration,

    #[clap(
        short,
        long,
        help = "Path to the dir to build in the inverted indexes.",
        default_value = "/tmp/toshokan_build"
    )]
    pub build_dir: String,

    #[clap(
        long,
        help = "Sets the amount of memory allocated for all indexing threads.
The memory is split evenly between all indexing threads, once a thread reaches its limit a commit is triggered.",
        default_value = "1073741824"
    )]
    pub memory_budget: usize,
}

#[derive(Parser, Debug, Clone)]
pub struct MergeArgs {
    #[clap(help = "The index name.")]
    pub name: String,

    #[clap(
        short,
        long,
        help = "Path to the dir to merge in the inverted indexes.",
        default_value = "/tmp/toshokan_merge"
    )]
    pub merge_dir: String,
}

#[derive(Parser, Debug, Clone)]
pub struct SearchArgs {
    #[clap(help = "The index name.")]
    pub name: String,

    #[clap(help = "Query in tantivy syntax.")]
    pub query: String,

    #[clap(
        short,
        long,
        default_value = "1",
        help = "Limit to a number of top results."
    )]
    pub limit: usize,
}

#[must_use]
pub fn parse_args() -> Args {
    Args::parse()
}
