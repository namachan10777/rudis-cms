use anyhow::Context;
use clap::Parser;
use futures::future::join_all;
use nothing_cms::{backend::cloudflare::CloudflareBackend, config::BackendVariants};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tracing::error;

#[derive(Parser)]
struct Opts {
    #[clap(short, long, env = "NOTHING_CMS_CONFIG")]
    config: PathBuf,
}

async fn run(opts: Opts) -> anyhow::Result<()> {
    let config = smol::fs::read_to_string(&opts.config).await?;
    let rootdir = opts
        .config
        .parent()
        .with_context(|| "no parent dir found")?;
    std::env::set_current_dir(rootdir)?;
    let config: nothing_cms::config::Config = serde_yaml::from_str(&config)?;
    for (_, collection) in config {
        match collection.backend {
            BackendVariants::Cloudflare(_) => {
                let schema: HashMap<String, nothing_cms::config::FieldDef<CloudflareBackend>> =
                    serde_json::from_value(collection.schema)?;
                let schema = nothing_cms::preprocess::Schema {
                    document_type: nothing_cms::preprocess::DocumentType::Markdown,
                    schema,
                };
                let schema = Arc::new(schema);
                let tasks = glob::glob(&collection.glob)?.map(|path| {
                    let schema = schema.clone();
                    async move {
                        let path = path?;
                        let src = smol::fs::read_to_string(&path).await?;
                        let doc = schema.preprocess_document(&path, &src).await?;
                        Ok::<_, anyhow::Error>(doc)
                    }
                });
                dbg!(join_all(tasks).await);
            }
        }
    }
    unimplemented!()
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
