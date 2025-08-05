use std::path::PathBuf;

use anyhow::{Context, anyhow};
use clap::Parser;
use tracing::error;

#[derive(Parser)]
struct Opts {
    #[clap(short, long, env = "NOTHING_CMS_CONFIG")]
    config: PathBuf,
}

async fn run(opts: Opts) -> anyhow::Result<()> {
    let config = smol::fs::read_to_string(&opts.config)
        .await
        .with_context(|| "read config")?;
    let config: nothing_cms::config::Config = serde_yaml::from_str(&config)
        .with_context(|| format!("parse config from {}", opts.config.display()))?;
    for config in config.values() {
        config.validate().map_err(|msg| anyhow!("{msg}"))?;
    }
    Ok(())
}

fn main() {
    let opts = Opts::parse();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    if let Err(e) = smol::block_on(run(opts)) {
        error!(?e, "ciritial error");
    }
}
