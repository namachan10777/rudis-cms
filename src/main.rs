use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context as _;
use clap::Parser;
use futures::future::try_join_all;
use indexmap::IndexMap;
use rudis_cms::progress::{
    BatchPhase, EntryStatus, ProgressReporter, UploadStatus, create_reporter,
};
use rudis_cms::{
    config, deploy, job,
    process_data::table::{Tables, Uploads},
    schema,
};

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

#[derive(clap::Args)]
struct CloudflareCredentials {
    #[clap(long, env = "CF_ACCOUNT_ID")]
    cf_account_id: String,
    #[clap(long, env = "CF_API_TOKEN")]
    cf_api_token: String,
    #[clap(long, env = "R2_ACCESS_KEY_ID")]
    r2_access_key_id: String,
    #[clap(long, env = "R2_SECRET_ACCESS_KEY")]
    r2_secret_access_key: String,
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
        #[clap(flatten)]
        creds: CloudflareCredentials,
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

/// State shared by all pipeline steps from CLI parsing through document
/// processing.
struct Pipeline {
    collection: config::Collection,
    schema: schema::CollectionSchema,
    hasher: blake3::Hasher,
    paths: Vec<PathBuf>,
    reporter: Arc<dyn ProgressReporter>,
}

impl Pipeline {
    /// Load config and compile the schema. Glob expansion is anchored at the
    /// directory containing the config file, so the process working directory
    /// is left untouched.
    async fn load(config: &Path, reporter: Arc<dyn ProgressReporter>) -> anyhow::Result<Self> {
        reporter.set_phase(BatchPhase::LoadingConfig);

        let config_path = config
            .canonicalize()
            .with_context(|| format!("canonicalize config path {}", config.display()))?;
        let basedir = config_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("config has no parent directory"))?
            .to_path_buf();

        let config_content = tokio::fs::read_to_string(&config_path)
            .await
            .with_context(|| format!("reading config {}", config_path.display()))?;

        let mut hasher = blake3::Hasher::new();
        hasher.update(config_content.as_bytes());

        let collection: config::Collection =
            serde_yaml::from_str(&config_content).context("parsing config YAML")?;

        reporter.set_phase(BatchPhase::CompilingSchema);
        let schema = schema::TableSchema::compile(&collection)?;

        let glob_pattern = resolve_glob_pattern(&basedir, &collection.glob);
        let paths: Vec<PathBuf> = glob::glob(&glob_pattern)
            .with_context(|| format!("invalid glob pattern: {glob_pattern}"))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(Self {
            collection,
            schema,
            hasher,
            paths,
            reporter,
        })
    }

    /// Process all documents matching the glob in parallel, returning the
    /// merged `Tables` and `Uploads`.
    async fn process_documents(&self) -> anyhow::Result<(Tables, Uploads)> {
        self.reporter.set_phase(BatchPhase::ProcessingDocuments);

        let entry_names: Vec<String> = self.paths.iter().map(|p| p.display().to_string()).collect();
        self.reporter.register_entries(entry_names);

        let tasks = self.paths.iter().map(|path| {
            let hasher = self.hasher.clone();
            let schema = &self.schema;
            let collection = &self.collection;
            let reporter = self.reporter.clone();
            async move {
                let path_str = path.display().to_string();
                reporter.update_entry(&path_str, EntryStatus::Processing);

                let (result, warnings) = rudis_cms::warning::collect_warnings(
                    rudis_cms::process_data::table::push_rows_from_document(
                        &collection.table,
                        hasher,
                        schema,
                        &collection.syntax,
                        path,
                    ),
                )
                .await;

                for warning in warnings {
                    reporter.add_entry_warning(&path_str, &warning);
                }

                let result = result.map(|(tables, mut uploads)| {
                    for upload in &mut uploads {
                        upload.source_entry = Some(path_str.clone());
                    }
                    (tables, uploads)
                });

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

        Ok((tables, uploads))
    }
}

fn resolve_glob_pattern(basedir: &Path, pattern: &str) -> String {
    if Path::new(pattern).is_absolute() {
        pattern.to_string()
    } else {
        basedir.join(pattern).to_string_lossy().into_owned()
    }
}

async fn build_cloudflare_executor(
    creds: &CloudflareCredentials,
    collection: &config::Collection,
) -> anyhow::Result<
    job::JobExecutor<
        deploy::cloudflare::d1::Client,
        deploy::cloudflare::kv::Client,
        deploy::cloudflare::r2::Client,
        deploy::cloudflare::asset::Client,
    >,
> {
    let kv = deploy::cloudflare::kv::Client::new(&creds.cf_account_id, &creds.cf_api_token);
    let d1 = deploy::cloudflare::d1::Client::new(
        creds.cf_account_id.clone(),
        creds.cf_api_token.clone(),
        collection.database_id.clone(),
    )
    .context("constructing D1 client")?;
    let r2 = deploy::cloudflare::r2::Client::new(
        &creds.cf_account_id,
        &creds.r2_access_key_id,
        &creds.r2_secret_access_key,
    )
    .await;
    let asset = deploy::cloudflare::asset::Client {};
    Ok(job::JobExecutor { kv, d1, r2, asset })
}

fn register_uploads(
    reporter: &Arc<dyn ProgressReporter>,
    uploads: &[rudis_cms::process_data::table::Upload],
    status: UploadStatus,
) {
    for upload in uploads {
        let entry = upload.source_entry.as_deref().unwrap_or("_unknown");
        let key = upload.pointer.to_string();
        reporter.register_upload(entry, &key);
        reporter.update_upload(&key, status.clone());
    }
}

async fn run_batch(
    config: &Path,
    force: bool,
    creds: CloudflareCredentials,
    reporter: Arc<dyn ProgressReporter>,
) -> anyhow::Result<()> {
    let pipeline = Pipeline::load(config, reporter.clone()).await?;
    let executor = build_cloudflare_executor(&creds, &pipeline.collection).await?;

    let (tables, uploads) = pipeline.process_documents().await?;

    reporter.set_phase(BatchPhase::UploadingStorage);

    let present_objects = executor.fetch_objects_metadata(&pipeline.schema).await?;
    let (to_upload, skipped) = job::partition_uploads(uploads, &present_objects, force);

    register_uploads(&reporter, &to_upload, UploadStatus::Uploading);
    register_uploads(&reporter, &skipped, UploadStatus::Skipped);

    executor
        .batch(&pipeline.schema, &tables, to_upload.clone(), force)
        .await?;

    for upload in &to_upload {
        let key = upload.pointer.to_string();
        reporter.update_upload(&key, UploadStatus::Uploaded);
    }

    reporter.set_phase(BatchPhase::Completed);
    reporter.finish();

    Ok(())
}

async fn run_dump(
    config: &Path,
    storage_path: &str,
    db_path: &str,
    reporter: Arc<dyn ProgressReporter>,
) -> anyhow::Result<()> {
    let pipeline = Pipeline::load(config, reporter.clone()).await?;

    reporter.log_info("Opening storage database...");
    let storage = deploy::local::storage::LocalStorage::open(storage_path).await?;

    reporter.log_info("Opening main database...");
    let db = deploy::local::db::LocalDatabase::open(db_path).await?;

    let executor = job::JobExecutor {
        kv: storage.kv_client(),
        d1: db.client(),
        r2: storage.r2_client(),
        asset: storage.asset_client(),
    };

    let (tables, uploads) = pipeline.process_documents().await?;

    reporter.set_phase(BatchPhase::SyncingDatabase);
    reporter.log_info("Dropping existing tables...");
    executor.drop_all_table_for_dump(&pipeline.schema).await?;

    reporter.set_phase(BatchPhase::UploadingStorage);
    register_uploads(&reporter, &uploads, UploadStatus::Uploading);

    executor
        .batch(&pipeline.schema, &tables, uploads.clone(), true)
        .await?;

    for upload in &uploads {
        let key = upload.pointer.to_string();
        reporter.update_upload(&key, UploadStatus::Uploaded);
    }

    reporter.set_phase(BatchPhase::Completed);
    reporter.finish();

    Ok(())
}

async fn run_show_schema(config: &Path, cmd: ShowSchemaCommand) -> anyhow::Result<()> {
    let config_content = tokio::fs::read_to_string(config).await?;
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
            save,
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
                tokio::fs::create_dir_all(&basedir).await?;
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

async fn run(opts: Opts) -> anyhow::Result<()> {
    let Opts { config, subcmd } = opts;
    match subcmd {
        SubCommand::ShowSchema { cmd } => run_show_schema(&config, cmd).await,
        SubCommand::Batch { force, creds } => {
            let reporter = create_reporter();
            run_batch(&config, force, creds, reporter.clone())
                .await
                .inspect_err(|e| {
                    reporter.set_phase(BatchPhase::Failed(e.to_string()));
                    reporter.finish();
                })
        }
        SubCommand::Dump { storage, db } => {
            let reporter = create_reporter();
            run_dump(&config, &storage, &db, reporter.clone())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_glob_is_anchored_at_basedir() {
        let pat = resolve_glob_pattern(Path::new("/tmp/site"), "posts/**/*.md");
        assert_eq!(pat, "/tmp/site/posts/**/*.md");
    }

    #[test]
    fn absolute_glob_is_passed_through() {
        let pat = resolve_glob_pattern(Path::new("/tmp/site"), "/abs/posts/**/*.md");
        assert_eq!(pat, "/abs/posts/**/*.md");
    }
}
