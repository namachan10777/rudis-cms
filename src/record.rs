use std::{
    hash::Hash,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, LazyLock},
};

use futures::future::try_join_all;
use indexmap::IndexMap;

use crate::{
    Error, ErrorContext, ErrorDetail, backend,
    config::{self, DocumentSyntax},
    field::{
        ColumnValue, CompoundId, CompoundIdPrefix,
        markdown::{self, compress},
        object_loader,
    },
    schema::{self, TableSchemas},
};

pub struct ImageColumn {}

pub struct FileColumn {}

pub struct ImageColumnVariantValue {
    pub url: url::Url,
    pub width: u32,
    pub height: u32,
    pub content_type: String,
}

pub struct ImageColumnValue {
    pub url: url::Url,
    pub width: u32,
    pub height: u32,
    pub content_type: String,
    pub hash: blake3::Hash,
    pub variants: Vec<ImageColumnValue>,
}

static FRONTMATTER_SEPARATOR_YAML: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"(?:^|\n)---\s*\n").unwrap());

static FRONTMATTER_SEPARATOR_TOML: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"(?:^|\n)\+\+\+\s*\n").unwrap());

fn parse_markdown<'c>(
    content: &'c str,
) -> Result<(serde_json::Map<String, serde_json::Value>, &'c str), ErrorDetail> {
    if let Some(start) = FRONTMATTER_SEPARATOR_YAML.find(&content) {
        if let Some(end) = FRONTMATTER_SEPARATOR_YAML.find_at(&content, start.end() + 1) {
            let frontmatter = serde_yaml::from_str(&content[start.end()..end.start()])
                .map_err(ErrorDetail::ParseYaml)?;
            Ok((frontmatter, &content[end.end()..]))
        } else {
            Err(ErrorDetail::UnclosedFrontmatter)
        }
    } else if let Some(start) = FRONTMATTER_SEPARATOR_TOML.find(&content) {
        if let Some(end) = FRONTMATTER_SEPARATOR_TOML.find_at(&content, start.end() + 1) {
            let frontmatter = toml::de::from_str(&content[start.end()..end.start()])
                .map_err(ErrorDetail::ParseToml)?;
            Ok((frontmatter, &content[end.end()..]))
        } else {
            Err(ErrorDetail::UnclosedFrontmatter)
        }
    } else {
        Ok((Default::default(), content))
    }
}

fn extract_id_value(
    name: &str,
    fields: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<String, ErrorDetail> {
    let Some(id) = fields.remove(name) else {
        return Err(ErrorDetail::MissingField(name.to_owned()));
    };
    match id {
        serde_json::Value::String(id) => Ok(id),
        _ => Err(ErrorDetail::TypeMismatch {
            expected: "string",
            got: id,
        }),
    }
}
fn is_normal_required_field(def: &schema::FieldType) -> bool {
    match def {
        schema::FieldType::Id => false,
        schema::FieldType::Hash => false,
        schema::FieldType::String { required, .. } => *required,
        schema::FieldType::Boolean { required, .. } => *required,
        schema::FieldType::Integer { required, .. } => *required,
        schema::FieldType::Real { required, .. } => *required,
        schema::FieldType::Date { required, .. } => *required,
        schema::FieldType::Datetime { required, .. } => *required,
        schema::FieldType::Image { required, .. } => *required,
        schema::FieldType::File { required, .. } => *required,
        schema::FieldType::Markdown { required, .. } => *required,
        schema::FieldType::Records { required, .. } => *required,
    }
}

struct Row {
    id: CompoundId,
    fields: IndexMap<String, FieldValue>,
    hash: blake3::Hash,
}

struct Records {
    table: String,
    rows: Vec<Row>,
}

enum FieldValue {
    Column(ColumnValue),
    Markdown {
        document: compress::RichTextDocument,
        storage: config::MarkdownStorage,
    },
    Records(Records),
}

struct RecordContext<'c, R> {
    table: String,
    schema: Arc<TableSchemas>,
    hasher: blake3::Hasher,
    compound_id_prefix: CompoundIdPrefix,
    error: ErrorContext,
    document_path: PathBuf,
    backend: &'c R,
}

impl<'c, R> Clone for RecordContext<'c, R> {
    fn clone(&self) -> Self {
        Self {
            table: self.table.clone(),
            schema: self.schema.clone(),
            hasher: self.hasher.clone(),
            compound_id_prefix: self.compound_id_prefix.clone(),
            error: self.error.clone(),
            document_path: self.document_path.clone(),
            backend: self.backend,
        }
    }
}

impl<'source, R> RecordContext<'source, R> {
    fn current_schema(&self) -> &Arc<schema::Schema> {
        self.schema.get(&self.table).unwrap()
    }

    fn nest(self, table: impl Into<String>, id: CompoundId) -> Result<Self, crate::Error> {
        let compound_id_prefix_names = self.current_schema().compound_id_prefix_names.clone();
        let Self {
            schema,
            error,
            document_path,
            backend,
            ..
        } = self;
        let compound_id_prefix = id
            .try_into_prefix(compound_id_prefix_names)
            .map_err(|detail| error.error(detail))?;
        Ok(Self {
            table: table.into(),
            hasher: self.hasher.clone(),
            schema,
            compound_id_prefix,
            error,
            document_path,
            backend,
        })
    }

    fn id(&self, id: impl Into<String>) -> CompoundId {
        self.compound_id_prefix
            .clone()
            .id(&self.current_schema().id_name, id.into())
    }
}

macro_rules! bail {
    ($ctx:expr, $detail:expr) => {
        return Err($ctx.error($detail))
    };
}

fn process_hash_field<'source, R: Send + Sync>(
    ctx: &RecordContext<'source, R>,
    name: &str,
) -> Result<ColumnValue, Error> {
    bail!(ctx.error, ErrorDetail::FoundComputedField(name.to_owned()))
}

fn process_boolean_field<'source, R: Send + Sync>(
    ctx: &RecordContext<'source, R>,
    value: serde_json::Value,
) -> Result<ColumnValue, Error> {
    if let serde_json::Value::Bool(b) = value {
        Ok(ColumnValue::Boolean(b))
    } else {
        bail!(
            &ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "boolean",
                got: value,
            }
        );
    }
}

fn process_integer_field<'source, R: Send + Sync>(
    ctx: &RecordContext<'source, R>,
    value: serde_json::Value,
) -> Result<ColumnValue, Error> {
    if let serde_json::Value::Number(n) = value {
        if n.is_i64() {
            Ok(ColumnValue::Number(n))
        } else {
            bail!(
                &ctx.error,
                ErrorDetail::TypeMismatch {
                    expected: "integer",
                    got: n.into(),
                }
            );
        }
    } else {
        bail!(
            &ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "integer",
                got: value,
            }
        );
    }
}

fn process_real_field<'source, R: Send + Sync>(
    ctx: &RecordContext<'source, R>,
    value: serde_json::Value,
) -> Result<ColumnValue, Error> {
    if let serde_json::Value::Number(n) = value {
        if n.is_f64() {
            Ok(ColumnValue::Number(n))
        } else {
            bail!(
                &ctx.error,
                ErrorDetail::TypeMismatch {
                    expected: "real",
                    got: n.into(),
                }
            );
        }
    } else {
        bail!(
            &ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "real",
                got: value,
            }
        );
    }
}

fn process_string_field<'source, R: Send + Sync>(
    ctx: &RecordContext<'source, R>,
    value: serde_json::Value,
) -> Result<ColumnValue, Error> {
    if let serde_json::Value::String(string) = value {
        Ok(ColumnValue::String(string))
    } else {
        bail!(
            &ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "string",
                got: value,
            }
        );
    }
}

fn process_date_field<'source, R: Send + Sync>(
    ctx: &RecordContext<'source, R>,
    value: serde_json::Value,
) -> Result<ColumnValue, Error> {
    if let serde_json::Value::String(date) = value {
        let date = date
            .parse::<chrono::NaiveDate>()
            .map_err(|_| ctx.error.error(ErrorDetail::InvalidDate(date.to_owned())))?;
        Ok(ColumnValue::Date(date))
    } else {
        bail!(
            &ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "date",
                got: value,
            }
        );
    }
}

fn process_datetime_field<'source, R: Send + Sync>(
    ctx: &RecordContext<'source, R>,
    value: serde_json::Value,
) -> Result<ColumnValue, Error> {
    if let serde_json::Value::String(datetime) = value {
        let datetime = datetime.parse::<chrono::NaiveDateTime>().map_err(|_| {
            ctx.error
                .error(ErrorDetail::InvalidDatetime(datetime.to_owned()))
        })?;
        Ok(ColumnValue::Datetime(datetime))
    } else {
        bail!(
            &ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "datetime",
                got: value,
            }
        );
    }
}

async fn process_records_field<'source, R: backend::RecordBackend + Sync + Send>(
    ctx: &RecordContext<'source, R>,
    id: &CompoundId,
    table: &str,
    value: serde_json::Value,
) -> Result<Vec<Row>, Error> {
    let serde_json::Value::Array(records) = value else {
        bail!(
            ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "array",
                got: value,
            }
        )
    };
    let ctx = ctx.clone().nest(table, id.clone())?;
    let tasks = records.into_iter().map(|record| async {
        match record {
            serde_json::Value::String(id) => {
                if ctx.current_schema().is_id_only_table() {
                    let id = ctx.id(id);
                    let mut hasher = blake3::Hasher::new();
                    hasher.update(b"id");
                    hasher.update(id.to_string().as_bytes());
                    Ok(Row {
                        id,
                        hash: hasher.finalize(),
                        fields: Default::default(),
                    })
                } else {
                    bail!(
                        ctx.error,
                        ErrorDetail::TypeMismatch {
                            expected: "object",
                            got: id.into()
                        }
                    )
                }
            }
            serde_json::Value::Object(fields) => process_row(&ctx, fields).await,
            _ => bail!(
                ctx.error,
                ErrorDetail::TypeMismatch {
                    expected: "string or object",
                    got: record,
                }
            ),
        }
    });
    let rows = try_join_all(tasks).await?;
    Ok(rows)
}

async fn process_image_field<'source, R: backend::RecordBackend + Sync + Send>(
    ctx: &RecordContext<'source, R>,
    hasher: &mut blake3::Hasher,
    id: &CompoundId,
    name: &str,
    transform: &config::ImageTransform,
    storage: &config::ImageStorage,
    value: serde_json::Value,
) -> Result<ColumnValue, Error> {
    let serde_json::Value::String(src) = value else {
        bail!(
            ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "string",
                got: value
            }
        )
    };
    let image = object_loader::load_image(&src, Some(&ctx.document_path))
        .await
        .map_err(ErrorDetail::LoadImage)
        .map_err(|error| ctx.error.error(error))?;
    hasher.update(image.hash.as_bytes());
    let value = ctx
        .backend
        .push_image(&ctx.table, name, id, transform, storage, image)
        .map_err(|detail| ctx.error.error(detail))?;
    Ok(ColumnValue::Image(value))
}

async fn process_file_field<'source, R: backend::RecordBackend + Send + Sync>(
    ctx: &RecordContext<'source, R>,
    hasher: &mut blake3::Hasher,
    name: &str,
    id: &CompoundId,
    storage: &config::FileStorage,
    value: serde_json::Value,
) -> Result<ColumnValue, Error> {
    let serde_json::Value::String(src) = value else {
        bail!(
            ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "string",
                got: value
            }
        )
    };
    let file = object_loader::load(&src, Some(&ctx.document_path))
        .await
        .map_err(ErrorDetail::Load)
        .map_err(|error| ctx.error.error(error))?;
    hasher.update(file.hash.as_bytes());
    let value = ctx
        .backend
        .push_file(&ctx.table, name, id, storage, file)
        .map_err(|detail| ctx.error.error(detail))?;
    Ok(ColumnValue::File(value))
}

async fn process_markdown_field<'source, R: backend::RecordBackend + Send + Sync>(
    ctx: &RecordContext<'source, R>,
    hasher: &mut blake3::Hasher,
    name: &str,
    id: &CompoundId,
    storage: &config::MarkdownStorage,
    _: &config::MarkdownConfig,
    image: &config::MarkdownImageConfig,
    value: serde_json::Value,
) -> Result<(FieldValue, blake3::Hash), Error> {
    let serde_json::Value::String(src) = value else {
        bail!(
            ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "string",
                got: value
            }
        )
    };
    let document = markdown::parser::parse(&src);
    let (document, hashes) = markdown::resolver::RichTextDocument::resolve(
        document,
        Some(&ctx.document_path),
        ctx.backend,
        &ctx.table,
        name,
        id,
        &image.transform,
        &image.storage,
        image.embed_svg_threshold,
    )
    .await
    .map_err(|detail| ctx.error.error(detail))?;
    let document = markdown::compress::compress(document);
    hashes.iter().for_each(|hash| {
        hasher.update(hash.as_bytes());
    });
    let value = FieldValue::Markdown {
        document,
        storage: storage.clone(),
    };
    Ok((value, hasher.finalize()))
}

async fn process_field<'source, R: backend::RecordBackend + Sync + Send>(
    ctx: &RecordContext<'source, R>,
    hasher: &mut blake3::Hasher,
    id: &CompoundId,
    name: &str,
    def: &schema::FieldType,
    value: Option<serde_json::Value>,
) -> Result<Option<FieldValue>, Error> {
    hasher.update(name.as_bytes());
    let value = match value {
        Some(value) => value,
        None => {
            if matches!(def, schema::FieldType::Id | schema::FieldType::Hash) {
                return Ok(None);
            }
            if is_normal_required_field(def) {
                bail!(&ctx.error, ErrorDetail::MissingField(name.to_owned()));
            } else {
                return Ok(Some(FieldValue::Column(ColumnValue::Null)));
            }
        }
    };
    let value = match def {
        schema::FieldType::Id => unreachable!(),
        schema::FieldType::Hash => process_hash_field(ctx, name).map(FieldValue::Column)?,
        schema::FieldType::Boolean { .. } => {
            process_boolean_field(ctx, value).map(FieldValue::Column)?
        }
        schema::FieldType::String { .. } => {
            process_string_field(ctx, value).map(FieldValue::Column)?
        }
        schema::FieldType::Integer { .. } => {
            process_integer_field(ctx, value).map(FieldValue::Column)?
        }
        schema::FieldType::Real { .. } => process_real_field(ctx, value).map(FieldValue::Column)?,
        schema::FieldType::Date { .. } => process_date_field(ctx, value).map(FieldValue::Column)?,
        schema::FieldType::Datetime { .. } => {
            process_datetime_field(ctx, value).map(FieldValue::Column)?
        }
        schema::FieldType::Image {
            transform, storage, ..
        } => process_image_field(ctx, hasher, id, name, transform, storage, value)
            .await
            .map(FieldValue::Column)?,
        schema::FieldType::File { storage, .. } => {
            process_file_field(ctx, hasher, name, id, storage, value)
                .await
                .map(FieldValue::Column)?
        }
        schema::FieldType::Markdown {
            image,
            config,
            storage,
            ..
        } => {
            let (value, hash) =
                process_markdown_field(ctx, hasher, name, id, storage, config, image, value)
                    .await?;
            hasher.update(hash.as_bytes());
            value
        }
        schema::FieldType::Records { table, .. } => {
            let rows = process_records_field(ctx, id, table, value).await?;
            FieldValue::Records(Records {
                table: table.clone(),
                rows,
            })
        }
    };

    Ok(Some(value))
}

async fn process_row_impl<'source, R: backend::RecordBackend + Sync + Send>(
    ctx: &RecordContext<'source, R>,
    mut fields: serde_json::Map<String, serde_json::Value>,
) -> Result<Row, Error> {
    let schema = ctx.current_schema();
    let id =
        extract_id_value(&schema.id_name, &mut fields).map_err(|detail| ctx.error.error(detail))?;
    let id = ctx.id(id);

    let ctx = RecordContext {
        error: ctx.error.with_id(id.clone()),
        ..ctx.clone()
    };

    let mut hasher = ctx.hasher.clone();

    let mut row = IndexMap::new();

    for (name, id) in id.pairs() {
        row.insert(name.into(), FieldValue::Column(ColumnValue::Id(id.into())));
    }

    for (name, def) in &schema.fields {
        let value = process_field(&ctx, &mut hasher, &id, name, def, fields.remove(name)).await?;
        if let Some(value) = value {
            row.insert(name.clone(), value);
        }
    }
    let hash = hasher.finalize();
    if let Some(hash_name) = &schema.hash_name {
        row.insert(
            hash_name.clone(),
            FieldValue::Column(ColumnValue::Hash(hash)),
        );
    }
    Ok(Row {
        id,
        fields: row,
        hash: hasher.finalize(),
    })
}

fn process_row<'source, 'c, R: backend::RecordBackend + Sync + Send>(
    ctx: &'c RecordContext<'source, R>,
    fields: serde_json::Map<String, serde_json::Value>,
) -> Pin<Box<dyn 'c + Future<Output = Result<Row, Error>>>> {
    Box::pin(process_row_impl(ctx, fields))
}

pub async fn push_rows_from_document<P: AsRef<Path>, R: backend::RecordBackend + Sync + Send>(
    table: &str,
    mut hasher: blake3::Hasher,
    schema: &schema::TableSchemas,
    syntax: &DocumentSyntax,
    backend: &R,
    path: P,
) -> Result<(), Error> {
    let ctx = ErrorContext::new(path.as_ref().to_owned());
    let document = smol::fs::read_to_string(&path)
        .await
        .map_err(|error| ctx.clone().error(ErrorDetail::ReadDocument(error)))?;
    hasher.update(document.as_bytes());
    let fields = match syntax {
        DocumentSyntax::Toml => toml::de::from_str(&document)
            .map_err(|error| ctx.error(ErrorDetail::ParseToml(error)))?,
        DocumentSyntax::Yaml => serde_yaml::from_str(&document)
            .map_err(|error| ctx.error(ErrorDetail::ParseYaml(error)))?,
        DocumentSyntax::Markdown { column } => {
            let (mut frontmatter, content) =
                parse_markdown(&document).map_err(|detail| ctx.error(detail))?;
            frontmatter.insert(column.clone(), content.to_owned().into());
            frontmatter
        }
    };
    let ctx = RecordContext {
        hasher,
        table: table.to_owned(),
        schema: Arc::new(schema.clone()),
        compound_id_prefix: Default::default(),
        error: ctx,
        document_path: path.as_ref().to_owned(),
        backend,
    };
    let row = process_row(&ctx, fields).await?;

    Ok(())
}
