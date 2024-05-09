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
}

#[derive(Parser, Debug, Clone)]
pub struct IndexArgs {
    #[clap(help = "Path to the input jsonl file you want to index.")]
    pub input_path: String,

    #[clap(help = "Path to the output index dir. Will create if doesn't exist.")]
    pub output_dir: String,
}

#[must_use]
pub fn parse_args() -> Args {
    Args::parse()
}
