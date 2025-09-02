use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use futures::future::try_join_all;
use indexmap::IndexMap;
use rudis_cms::{
    config, record, schema,
    sql::{DDL, create_ctx},
};
use tracing::{error, info, trace};

#[derive(clap::Subcommand)]
enum SubCommand {
    Batch,
    ShowSchema,
    Local {
        #[clap(short, long)]
        path: Option<PathBuf>,
    },
}

#[derive(clap::Parser)]
struct Opts {
    #[clap(short, long)]
    config: PathBuf,
    #[clap(subcommand)]
    subcmd: SubCommand,
}

struct Noop {}
impl rudis_cms::sql::StorageBackend for Noop {
    async fn delete(
        &self,
        _: impl Iterator<Item = rudis_cms::sql::R2Delete>,
        _: impl Iterator<Item = rudis_cms::sql::KvDelete>,
        _: impl Iterator<Item = rudis_cms::sql::AssetDelete>,
    ) -> Result<(), rudis_cms::sql::Error> {
        Ok(())
    }

    async fn upload(
        &self,
        _: impl Iterator<Item = rudis_cms::field::upload::R2Upload>,
        _: impl Iterator<Item = rudis_cms::field::upload::KvUpload>,
        _: impl Iterator<Item = rudis_cms::field::upload::AssetUpload>,
    ) -> Result<(), rudis_cms::sql::Error> {
        Ok(())
    }
}

async fn run(opts: Opts) -> anyhow::Result<()> {
    match opts.subcmd {
        SubCommand::ShowSchema => {
            let config = smol::fs::read_to_string(&opts.config).await?;
            let config: IndexMap<String, config::Collection> = serde_yaml::from_str(&config)?;
            for (name, collection) in &config {
                let schema = schema::Schema::tables(&collection)?;
                let liquid_ctx = create_ctx(&schema);
                println!("-- Table: {}", name);
                println!("{}", DDL.render(&liquid_ctx).unwrap());
            }
            Ok(())
        }
        SubCommand::Local { path } => {
            let mut hasher = blake3::Hasher::new();
            let config = smol::fs::read_to_string(&opts.config).await?;
            hasher.update(config.as_bytes());
            let config: IndexMap<String, config::Collection> = serde_yaml::from_str(&config)?;
            let conn = if let Some(path) = path {
                rusqlite::Connection::open(path)?
            } else {
                rusqlite::Connection::open_in_memory()?
            };
            for (_, collection) in &config {
                let schema = schema::Schema::tables(&collection)?;
                let liquid_ctx = create_ctx(&schema);
                conn.execute_batch(&DDL.render(&liquid_ctx).unwrap())?;
                let uploads = rudis_cms::field::upload::UploadCollector::default();
                let mut tables: record::Tables = IndexMap::new();
                let tasks = glob::glob(&collection.glob)?.into_iter().map(|path| async {
                    record::push_rows_from_document(
                        &collection.table,
                        hasher.clone(),
                        &schema,
                        &collection.syntax,
                        &uploads,
                        path?,
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))
                    .inspect(|tables| {
                        for (table, rows) in tables {
                            for row in rows {
                                trace!(table, ?row, "row");
                            }
                        }
                    })
                });
                try_join_all(tasks).await?.into_iter().for_each(|t| {
                    for (table, mut rows) in t {
                        tables.entry(table).or_default().append(&mut rows);
                    }
                });
                let uploads = uploads.collect().await;
                rudis_cms::sql::batch(&conn, &schema, tables, uploads, &Noop {}).await?;
            }
            Ok(())
        }
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
                let uploads = rudis_cms::field::upload::UploadCollector::default();
                let mut tables: record::Tables = IndexMap::new();
                for path in glob::glob(&collection.glob)? {
                    for (table, mut rows) in record::push_rows_from_document(
                        &collection.table,
                        hasher.clone(),
                        &schema,
                        &collection.syntax,
                        &uploads,
                        path?,
                    )
                    .await?
                    {
                        tables.entry(table).or_default().append(&mut rows);
                    }
                }
                let uploads = uploads.collect().await;
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
