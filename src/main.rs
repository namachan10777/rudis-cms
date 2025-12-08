use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use futures::future::try_join_all;
use indexmap::IndexMap;
use rudis_cms::progress::{
    BatchPhase, EntryStatus, ProgressReporter, UploadStatus, create_reporter,
};
use rudis_cms::{config, deploy, job, schema};

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
#[command(version, about, long_about = None)]
struct Opts {
    #[clap(short, long)]
    config: PathBuf,
    #[clap(subcommand)]
    subcmd: SubCommand,
}

async fn run_batch(
    opts: &Opts,
    force: bool,
    reporter: Arc<dyn ProgressReporter>,
) -> anyhow::Result<()> {
    reporter.set_phase(BatchPhase::LoadingConfig);

    let mut hasher = blake3::Hasher::new();
    let config_content = tokio::fs::read_to_string(&opts.config).await?;
    let config_path = opts.config.canonicalize()?;
    let basedir = config_path.parent();
    hasher.update(config_content.as_bytes());
    let collection: config::Collection = serde_yaml::from_str(&config_content)?;

    let cf_account_id = std::env::var("CF_ACCOUNT_ID")?;
    let cf_api_token = std::env::var("CF_API_TOKEN")?;
    let r2_access_key_id = std::env::var("R2_ACCESS_KEY_ID")?;
    let r2_secret_access_key = std::env::var("R2_SECRET_ACCESS_KEY")?;

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

    reporter.set_phase(BatchPhase::CompilingSchema);
    let compiled_schema = schema::TableSchema::compile(&collection)?;

    reporter.set_phase(BatchPhase::ProcessingDocuments);

    // Collect all paths first
    let paths: Vec<PathBuf> = glob::glob(&collection.glob)?
        .filter_map(|r| r.ok())
        .collect();

    let entry_names: Vec<String> = paths.iter().map(|p| p.display().to_string()).collect();
    reporter.register_entries(entry_names);

    let tasks = paths.into_iter().map(|path| {
        let hasher = hasher.clone();
        let compiled_schema = &compiled_schema;
        let collection = &collection;
        let reporter = reporter.clone();
        async move {
            let path_str = path.display().to_string();
            reporter.update_entry(&path_str, EntryStatus::Processing);

            let (result, warnings) = rudis_cms::warning::collect_warnings(
                rudis_cms::process_data::table::push_rows_from_document(
                    &collection.table,
                    hasher,
                    compiled_schema,
                    &collection.syntax,
                    &path,
                ),
            )
            .await;

            // Report collected warnings
            for warning in warnings {
                reporter.add_entry_warning(&path_str, &warning);
            }

            match &result {
                Ok(_) => reporter.update_entry(&path_str, EntryStatus::Done),
                Err(e) => reporter.update_entry(&path_str, EntryStatus::Failed(e.to_string())),
            }

            result.map_err(anyhow::Error::from)
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

    reporter.set_phase(BatchPhase::UploadingStorage);

    // Register all uploads (without entry association for now)
    for upload in &uploads {
        let key = upload.pointer.to_string();
        reporter.register_upload("_global", &key);
        reporter.update_upload(&key, UploadStatus::Uploading);
    }

    executor
        .batch(&compiled_schema, &tables, uploads.clone(), force)
        .await?;

    // Mark all uploads as done
    for upload in &uploads {
        let key = upload.pointer.to_string();
        reporter.update_upload(&key, UploadStatus::Done);
    }

    reporter.set_phase(BatchPhase::Completed);
    reporter.finish();

    Ok(())
}

async fn run_dump(
    opts: &Opts,
    storage_path: &str,
    db_path: &str,
    reporter: Arc<dyn ProgressReporter>,
) -> anyhow::Result<()> {
    reporter.set_phase(BatchPhase::LoadingConfig);

    let mut hasher = blake3::Hasher::new();
    let config_content = tokio::fs::read_to_string(&opts.config).await?;
    let config_path = opts.config.canonicalize()?;
    let basedir = config_path.parent();
    hasher.update(config_content.as_bytes());
    let collection: config::Collection = serde_yaml::from_str(&config_content)?;

    reporter.log_info("Opening storage database...");
    let storage = deploy::local::storage::LocalStorage::open(storage_path).await?;

    reporter.log_info("Opening main database...");
    let db = deploy::local::db::LocalDatabase::open(db_path).await?;

    let executor = rudis_cms::job::JobExecutor {
        kv: storage.kv_client(),
        d1: db.client(),
        r2: storage.r2_client(),
        asset: storage.asset_client(),
    };

    if let Some(basedir) = basedir {
        std::env::set_current_dir(basedir)?;
    }

    reporter.set_phase(BatchPhase::CompilingSchema);
    let compiled_schema = schema::TableSchema::compile(&collection)?;

    reporter.set_phase(BatchPhase::ProcessingDocuments);

    // Collect all paths first
    let paths: Vec<PathBuf> = glob::glob(&collection.glob)?
        .filter_map(|r| r.ok())
        .collect();

    let entry_names: Vec<String> = paths.iter().map(|p| p.display().to_string()).collect();
    reporter.register_entries(entry_names);

    let tasks = paths.into_iter().map(|path| {
        let hasher = hasher.clone();
        let compiled_schema = &compiled_schema;
        let collection = &collection;
        let reporter = reporter.clone();
        async move {
            let path_str = path.display().to_string();
            reporter.update_entry(&path_str, EntryStatus::Processing);

            let (result, warnings) = rudis_cms::warning::collect_warnings(
                rudis_cms::process_data::table::push_rows_from_document(
                    &collection.table,
                    hasher,
                    compiled_schema,
                    &collection.syntax,
                    &path,
                ),
            )
            .await;

            // Report collected warnings
            for warning in warnings {
                reporter.add_entry_warning(&path_str, &warning);
            }

            match &result {
                Ok(_) => reporter.update_entry(&path_str, EntryStatus::Done),
                Err(e) => reporter.update_entry(&path_str, EntryStatus::Failed(e.to_string())),
            }

            result.map_err(anyhow::Error::from)
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

    reporter.set_phase(BatchPhase::SyncingDatabase);
    reporter.log_info("Dropping existing tables...");
    executor.drop_all_table_for_dump(&compiled_schema).await?;

    reporter.set_phase(BatchPhase::UploadingStorage);

    // Register all uploads (without entry association for now)
    for upload in &uploads {
        let key = upload.pointer.to_string();
        reporter.register_upload("_global", &key);
        reporter.update_upload(&key, UploadStatus::Uploading);
    }

    executor
        .batch(&compiled_schema, &tables, uploads.clone(), true)
        .await?;

    // Mark all uploads as done
    for upload in &uploads {
        let key = upload.pointer.to_string();
        reporter.update_upload(&key, UploadStatus::Done);
    }

    reporter.set_phase(BatchPhase::Completed);
    reporter.finish();

    Ok(())
}

async fn run(opts: Opts) -> anyhow::Result<()> {
    match opts.subcmd {
        SubCommand::ShowSchema { cmd } => {
            let config_content = tokio::fs::read_to_string(&opts.config).await?;
            let collection: config::Collection = serde_yaml::from_str(&config_content)?;
            let name = &collection.name;
            match cmd {
                ShowSchemaCommand::Sql {
                    upsert,
                    cleanup,
                    fetch_objects,
                } => {
                    let compiled_schema = schema::TableSchema::compile(&collection)?;
                    println!("{}", job::sql::ddl(&compiled_schema));
                    if upsert {
                        for (table, table_schema) in &compiled_schema.tables {
                            println!("-- {name}:{table}: upsert.sql");
                            println!("{}", job::sql::upsert(table, table_schema));
                        }
                    }
                    if cleanup {
                        println!("-- {name}: cleanup.sql");
                        for (table, table_schema) in &compiled_schema.tables {
                            println!("-- {name}:{table}: cleanup.sql");
                            println!("{}", job::sql::cleanup(table, table_schema));
                        }
                    }
                    if fetch_objects {
                        println!("-- {name}: fetch_object.sql");
                        println!("{}", job::sql::fetch_objects(&compiled_schema));
                    }
                }
                ShowSchemaCommand::Typescript {
                    print,
                    ref save,
                    valibot,
                } => {
                    if print {
                        let compiled_schema = schema::TableSchema::compile(&collection)?;
                        let files = rudis_cms::typescript::file_map(&compiled_schema, valibot);
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
                        let compiled_schema = schema::TableSchema::compile(&collection)?;
                        let files = rudis_cms::typescript::file_map(&compiled_schema, valibot);
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
            let reporter = create_reporter();
            run_batch(&opts, force, reporter.clone())
                .await
                .inspect_err(|e| {
                    reporter.set_phase(BatchPhase::Failed(e.to_string()));
                    reporter.finish();
                })
        }
        SubCommand::Dump {
            ref storage,
            ref db,
        } => {
            let reporter = create_reporter();
            run_dump(&opts, storage, db, reporter.clone())
                .await
                .inspect_err(|e| {
                    reporter.set_phase(BatchPhase::Failed(e.to_string()));
                    reporter.finish();
                })
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    run(opts).await
}
