// test
#![forbid(unsafe_code)]

use std::process::ExitCode;

use clap::Parser;
use ralph_burning::cli::{self, Cli};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    init_tracing();

    let cli = Cli::parse();

    match cli::run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .compact()
        .with_target(false)
        .without_time()
        .with_writer(std::io::stderr)
        .try_init();
}
