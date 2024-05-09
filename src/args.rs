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

    #[clap(help = "Path to the output index dir. Will create if doesn't exist.")]
    pub output_dir: String,
}

#[derive(Parser, Debug, Clone)]
pub struct SearchArgs {
    #[clap(help = "Path to the index dir.")]
    pub input_dir: String,

    #[clap(help = "Query in tantivy syntax.")]
    pub query: String,
}

#[must_use]
pub fn parse_args() -> Args {
    Args::parse()
}
