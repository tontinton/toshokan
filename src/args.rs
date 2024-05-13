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

    #[clap(name = "search")]
    Search(SearchArgs),
}

#[derive(Parser, Debug, Clone)]
pub struct IndexArgs {
    #[clap(help = "Path to the input jsonl file you want to index.")]
    pub input_path: String,

    #[clap(help = "Path to the output index file.")]
    pub output_path: String,

    #[clap(
        short,
        long,
        help = "Path to the dir to build in the inverted indexes.",
        default_value = "/tmp/toshokan"
    )]
    pub build_dir: String,

    #[clap(short, long, help = "Merge all created segments into one segment.")]
    pub merge: bool,
}

#[derive(Parser, Debug, Clone)]
pub struct SearchArgs {
    #[clap(help = "Path to the unified index file.")]
    pub input_file: String,

    #[clap(help = "Footer metadata section length in the unified index file.")]
    pub footer: usize,

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
