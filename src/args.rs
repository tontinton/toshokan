use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[clap(subcommand)]
    pub subcmd: SubCommand,
}

#[derive(Parser, Debug, Clone)]
pub enum SubCommand {
    #[clap(name = "index")]
    Index(IndexArgs),

    #[clap(name = "merge")]
    Merge(MergeArgs),

    #[clap(name = "search")]
    Search(SearchArgs),
}

#[derive(Parser, Debug, Clone)]
pub struct IndexArgs {
    #[clap(help = "Path to the input jsonl file you want to index.")]
    pub input_path: String,

    #[clap(help = "Path to the index dir.")]
    pub index_dir: String,

    #[clap(
        short,
        long,
        help = "Path to the dir to build in the inverted indexes.",
        default_value = "/tmp/toshokan"
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
    #[clap(help = "Path to the index dir.")]
    pub index_dir: String,

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
    #[clap(help = "Path to the index dir.")]
    pub index_dir: String,

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
