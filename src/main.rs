use std::path::PathBuf;

use clap::Parser;
use futures::future::try_join_all;
use indexmap::IndexMap;
use rudis_cms::{config, deploy, job, schema};
use tracing::{error, info};

#[derive(clap::Subcommand)]
enum ShowSchemaCommand {
    Typescript {
        #[clap(short, long, required_unless_present = "save")]
        print: bool,
        #[clap(short, long)]
        save: Option<PathBuf>,
        #[clap(long)]
        valibot: bool,
    },
    Sql {
        #[clap(long)]
        upsert: bool,
        #[clap(long)]
        cleanup: bool,
        #[clap(long)]
        fetch_objects: bool,
    },
}

#[derive(clap::Subcommand)]
enum SubCommand {
    ShowSchema {
        #[clap(subcommand)]
        cmd: ShowSchemaCommand,
    },
    Batch {
        #[clap(short, long)]
        force: bool,
    },
    Dump {
        #[clap(long)]
        storage: String,
        #[clap(long)]
        db: String,
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
        SubCommand::ShowSchema { cmd } => {
            let config = tokio::fs::read_to_string(&opts.config).await?;
            let collection: config::Collection = serde_yaml::from_str(&config)?;
            let name = &collection.name;
            match cmd {
                ShowSchemaCommand::Sql {
                    upsert,
                    cleanup,
                    fetch_objects,
                } => {
                    let schema = schema::TableSchema::compile(&collection)?;
                    println!("{}", job::sql::ddl(&schema));
                    if upsert {
                        for (table, schema) in &schema.tables {
                            println!("-- {name}:{table}: upsert.sql");
                            println!("{}", job::sql::upsert(table, schema));
                        }
                    }
                    if cleanup {
                        println!("-- {name}: cleanup.sql");
                        for (table, schema) in &schema.tables {
                            println!("-- {name}:{table}: cleanup.sql");
                            println!("{}", job::sql::cleanup(table, schema));
                        }
                    }
                    if fetch_objects {
                        println!("-- {name}: fetch_object.sql");
                        println!("{}", job::sql::fetch_objects(&schema));
                    }
                }
                ShowSchemaCommand::Typescript {
                    print,
                    ref save,
                    valibot,
                } => {
                    if print {
                        let schema = schema::TableSchema::compile(&collection)?;
                        let files = rudis_cms::typescript::file_map(&schema, valibot);
                        for (_, content) in &files {
                            println!("// {name}");
                            print!("{content}");
                        }
                    }
                    if let Some(basedir) = save {
                        tokio::fs::create_dir_all(basedir).await?;
                        tokio::fs::write(
                            basedir.join("rudis.ts"),
                            include_str!("typescript/rudis.ts"),
                        )
                        .await?;
                        if valibot {
                            tokio::fs::write(
                                basedir.join("rudis-valibot.ts"),
                                include_str!("typescript/rudis-valibot.ts"),
                            )
                            .await?;
                        }
                        let schema = schema::TableSchema::compile(&collection)?;
                        let files = rudis_cms::typescript::file_map(&schema, valibot);
                        tokio::fs::create_dir_all(basedir.join(name)).await?;
                        for (filename, content) in &files {
                            let path = basedir.join(name).join(filename);
                            tokio::fs::write(&path, content).await?;
                        }
                    }
                }
            }
            Ok(())
        }
        SubCommand::Batch { force } => {
            let mut hasher = blake3::Hasher::new();
            let config = tokio::fs::read_to_string(&opts.config).await?;
            let config_path = opts.config.canonicalize()?;
            let basedir = config_path.parent();
            hasher.update(config.as_bytes());
            let collection: config::Collection = serde_yaml::from_str(&config)?;

            let cf_account_id = std::env::var("CF_ACCOUNT_ID").unwrap();
            let cf_api_token = std::env::var("CF_API_TOKEN").unwrap();
            let r2_access_key_id = std::env::var("R2_ACCESS_KEY_ID").unwrap();
            let r2_secret_access_key = std::env::var("R2_SECRET_ACCESS_KEY").unwrap();

            let kv = rudis_cms::deploy::cloudflare::kv::Client::new(&cf_account_id, &cf_api_token);
            let d1 = rudis_cms::deploy::cloudflare::d1::Client::new(
                cf_account_id.clone(),
                cf_api_token.clone(),
                collection.database_id.clone(),
            )?;
            let r2 = rudis_cms::deploy::cloudflare::r2::Client::new(
                &cf_account_id,
                &r2_access_key_id,
                &r2_secret_access_key,
            )
            .await;
            let asset = rudis_cms::deploy::cloudflare::asset::Client {};
            let executor = rudis_cms::job::JobExecutor { kv, d1, r2, asset };

            if let Some(basedir) = basedir {
                std::env::set_current_dir(basedir)?;
            }
            let schema = schema::TableSchema::compile(&collection)?;

            let tasks = glob::glob(&collection.glob)?.map(|path| {
                let hasher = hasher.clone();
                let schema = &schema;
                let collection = &collection;
                async move {
                    let path = path?;
                    rudis_cms::process_data::table::push_rows_from_document(
                        &collection.table,
                        hasher,
                        schema,
                        &collection.syntax,
                        path,
                    )
                    .await
                    .map_err(anyhow::Error::from)
                }
            });
            let mut tables = IndexMap::<_, Vec<_>>::new();
            let mut uploads = Vec::default();
            for (table_flakes, mut upload_flakes) in try_join_all(tasks).await? {
                for (table, mut rows) in table_flakes {
                    tables.entry(table).or_default().append(&mut rows);
                }
                uploads.append(&mut upload_flakes);
            }

            executor.batch(&schema, &tables, uploads, force).await?;
            Ok(())
        }
        SubCommand::Dump { storage, db } => {
            let mut hasher = blake3::Hasher::new();
            let config = tokio::fs::read_to_string(&opts.config).await?;
            let config_path = opts.config.canonicalize()?;
            let basedir = config_path.parent();
            hasher.update(config.as_bytes());
            let collection: config::Collection = serde_yaml::from_str(&config)?;
            info!("config loaded");
            let storage = deploy::local::storage::LocalStorage::open(&storage).await?;
            info!("storage db created");
            let db = deploy::local::db::LocalDatabase::open(&db).await?;
            info!("main db created");

            let executor = rudis_cms::job::JobExecutor {
                kv: storage.kv_client(),
                d1: db.client(),
                r2: storage.r2_client(),
                asset: storage.asset_client(),
            };

            if let Some(basedir) = basedir {
                std::env::set_current_dir(basedir)?;
            }
            let schema = schema::TableSchema::compile(&collection)?;
            info!("schema compiled");

            let tasks = glob::glob(&collection.glob)?.map(|path| {
                let hasher = hasher.clone();
                let schema = &schema;
                let collection = &collection;
                async move {
                    let path = path?;
                    rudis_cms::process_data::table::push_rows_from_document(
                        &collection.table,
                        hasher,
                        schema,
                        &collection.syntax,
                        path,
                    )
                    .await
                    .map_err(anyhow::Error::from)
                }
            });
            let mut tables = IndexMap::<_, Vec<_>>::new();
            let mut uploads = Vec::default();
            for (table_flakes, mut upload_flakes) in try_join_all(tasks).await? {
                for (table, mut rows) in table_flakes {
                    tables.entry(table).or_default().append(&mut rows);
                }
                uploads.append(&mut upload_flakes);
            }
            info!("all data prepared");

            executor.drop_all_table_for_dump(&schema).await?;
            executor.batch(&schema, &tables, uploads, true).await?;
            info!("batch completed");
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
