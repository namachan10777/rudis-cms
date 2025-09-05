use std::path::PathBuf;

use clap::Parser;
use indexmap::IndexMap;
use rudis_cms::{
    config, schema,
    sql::{SQL_DDL, liquid_default_context},
};
use tracing::error;

#[derive(clap::Subcommand)]
enum SubCommand {
    ShowSchema,
    Batch {
        #[clap(short, long)]
        force: bool,
    },
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
        SubCommand::ShowSchema => {
            let config = tokio::fs::read_to_string(&opts.config).await?;
            let config: IndexMap<String, config::Collection> = serde_yaml::from_str(&config)?;
            for (name, collection) in &config {
                let schema = schema::TableSchema::compile(collection)?;
                let liquid_ctx = liquid_default_context(&schema);
                println!("-- Table: {}", name);
                println!("{}", SQL_DDL.render(&liquid_ctx).unwrap());
            }
            Ok(())
        }
        SubCommand::Batch { force } => {
            let mut hasher = blake3::Hasher::new();
            let config = tokio::fs::read_to_string(&opts.config).await?;
            hasher.update(config.as_bytes());
            let config: IndexMap<String, config::Collection> = serde_yaml::from_str(&config)?;

            let cf_account_id = std::env::var("CF_ACCOUNT_ID").unwrap();
            let cf_api_token = std::env::var("CF_API_TOKEN").unwrap();
            let r2_access_key_id = std::env::var("R2_ACCESS_KEY_ID").unwrap();
            let r2_secret_access_key = std::env::var("R2_SECRET_ACCESS_KEY").unwrap();

            let storage = rudis_cms::job::cloudflrae::CloudflareStorage::new(
                &cf_account_id,
                &cf_api_token,
                &r2_access_key_id,
                &r2_secret_access_key,
            )
            .await;

            for (_, collection) in &config {
                let database = rudis_cms::job::cloudflrae::D1Database::new(
                    &cf_account_id,
                    &cf_api_token,
                    &collection.database_id,
                );

                rudis_cms::batch(&storage, &database, collection, hasher.clone(), force).await?;
            }
            Ok(())
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let opts = Opts::parse();
    run(opts).await.inspect_err(|e| error!(%e, "ciritical"))
}
