use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use indexmap::IndexMap;
use rudis_cms::{backend, config, record, schema};
use tracing::{error, info};

#[derive(clap::Subcommand)]
enum SubCommand {
    Batch,
}

#[derive(clap::Parser)]
struct Opts {
    #[clap(short, long)]
    config: PathBuf,
    #[clap(subcommand)]
    subcmd: SubCommand,
}

async fn run(opts: Opts) -> anyhow::Result<()> {
    match opts.subcmd {
        SubCommand::Batch => {
            let mut hasher = blake3::Hasher::new();
            let config = smol::fs::read_to_string(&opts.config).await?;
            hasher.update(config.as_bytes());
            let config: IndexMap<String, config::Collection> = serde_yaml::from_str(&config)?;
            if let Some(basedir) = opts
                .config
                .canonicalize()
                .with_context(|| "canonicalize config path")?
                .parent()
            {
                std::env::set_current_dir(basedir).with_context(|| "switch basedir")?;
            }
            for (name, collection) in config {
                info!(name, glob = collection.glob, "start");
                let schema = schema::Schema::tables(&collection)?;
                let backend = backend::debug::DebugBackend::default();
                for path in glob::glob(&collection.glob)? {
                    record::push_rows_from_document(
                        &collection.table,
                        hasher.clone(),
                        &schema,
                        &collection.syntax,
                        &backend,
                        path?,
                    )
                    .await?;
                }
            }
            Ok(())
        }
    }
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let opts = Opts::parse();
    smol::block_on(async move { run(opts).await.inspect_err(|e| error!(%e, "ciritical")) })
}
