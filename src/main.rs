use anyhow::Context;
use clap::Parser;
use futures::future::join_all;
use nothing_cms::{
    backend::{Backend, cloudflare::CloudflareBackend},
    config::BackendVariants,
};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tracing::error;

#[derive(Parser)]
enum SubCommand {
    ShowSchema,
}

#[derive(Parser)]
struct Opts {
    #[clap(short, long, env = "NOTHING_CMS_CONFIG")]
    config: PathBuf,
    #[clap(subcommand)]
    subcmd: SubCommand,
}

async fn init_backend<B: Backend>(
    backend: B::BackendConfig,
    schema: serde_json::Value,
    glob: &str,
) -> anyhow::Result<B> {
    let schema: HashMap<String, nothing_cms::config::FieldDef<B>> = serde_json::from_value(schema)?;
    let schema = nothing_cms::preprocess::Schema {
        document_type: nothing_cms::preprocess::DocumentType::Markdown,
        schema,
    };
    let schema = Arc::new(schema);
    let tasks = glob::glob(glob)?.map(|path| {
        let schema = schema.clone();
        async move {
            let path = path?;
            let src = smol::fs::read_to_string(&path).await?;
            let doc = schema.preprocess_document(&path, &src).await?;
            Ok::<_, anyhow::Error>(doc)
        }
    });
    let documents = join_all(tasks).await;
    B::init(backend, schema).await.map_err(Into::into)
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
            BackendVariants::Cloudflare(backend) => {
                let backend =
                    init_backend::<CloudflareBackend>(backend, collection.schema, &collection.glob)
                        .await?;
                match opts.subcmd {
                    SubCommand::ShowSchema => {
                        print!("{}", backend.print_schema());
                    }
                }
            }
        };
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
