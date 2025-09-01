use std::{
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, LazyLock},
};

use futures::future::try_join_all;
use indexmap::IndexMap;

use crate::{
    Error, ErrorContext, ErrorDetail,
    backend::{self, RecordBackend},
    config::{self, DocumentSyntax, MarkdownStorage},
    field::{
        ColumnValue, CompoundId, CompoundIdPrefix, MarkdownReference, markdown, object_loader,
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

#[derive(Default)]
struct Row {
    fields: IndexMap<String, ColumnValue>,
}

impl Row {
    fn with_compount_id(self, id: &CompoundId) -> Self {
        Self {
            fields: id.clone().assign_to_row(self.fields),
        }
    }
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

struct RecordContext<'c, R> {
    table: String,
    schema: Arc<TableSchemas>,
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

async fn process_records_field_impl<'source, R: RecordBackend + Sync + Send>(
    ctx: &RecordContext<'source, R>,
    id: &CompoundId,
    table: &str,
    records: Vec<serde_json::Value>,
) -> Result<(), Error> {
    let ctx = ctx.clone().nest(table, id.clone())?;
    let tasks = records.into_iter().map(|record| async {
        match record {
            serde_json::Value::String(id) => {
                if ctx.current_schema().is_id_only_table() {
                    let id = ctx.id(id);
                    let row = Row::default().with_compount_id(&id);
                    ctx.backend.push_row(table, row.fields);
                    Ok(())
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
            serde_json::Value::Object(fields) => {
                push_rows(&ctx, fields).await?;
                Ok(())
            }
            _ => bail!(
                ctx.error,
                ErrorDetail::TypeMismatch {
                    expected: "string or object",
                    got: record,
                }
            ),
        }
    });
    try_join_all(tasks).await?;
    Ok(())
}

async fn process_records_field<'source, R: RecordBackend + Sync + Send>(
    ctx: &RecordContext<'source, R>,
    id: &CompoundId,
    table: &str,
    value: serde_json::Value,
) -> Result<(), Error> {
    if let serde_json::Value::Array(records) = value {
        process_records_field_impl(ctx, id, table, records).await
    } else {
        bail!(
            ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "array",
                got: value,
            }
        )
    }
}
async fn process_image_field_impl<'source, R: RecordBackend + Sync + Send>(
    ctx: &RecordContext<'source, R>,
    name: &str,
    transform: &config::ImageTransform,
    storage: &config::ImageStorage,
    src: &str,
) -> Result<ColumnValue, Error> {
    let image = object_loader::load_image(&src, Some(&ctx.document_path))
        .await
        .map_err(ErrorDetail::LoadImage)
        .map_err(|error| ctx.error.error(error))?;
    let value = ctx
        .backend
        .push_image(&ctx.table, name, transform, storage, image)
        .map_err(|detail| ctx.error.error(detail))?;
    Ok(ColumnValue::Image(value))
}

async fn process_image_field<'source, R: RecordBackend + Sync + Send>(
    ctx: &RecordContext<'source, R>,
    name: &str,
    transform: &config::ImageTransform,
    storage: &config::ImageStorage,
    value: serde_json::Value,
) -> Result<ColumnValue, Error> {
    if let serde_json::Value::String(src) = value {
        process_image_field_impl(ctx, name, transform, storage, &src).await
    } else {
        bail!(
            ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "string",
                got: value
            }
        )
    }
}

async fn process_file_field_impl<'source, R: RecordBackend + Send + Sync>(
    ctx: &RecordContext<'source, R>,
    name: &str,
    storage: &config::FileStorage,
    src: &str,
) -> Result<ColumnValue, Error> {
    let image = object_loader::load(&src, Some(&ctx.document_path))
        .await
        .map_err(ErrorDetail::Load)
        .map_err(|error| ctx.error.error(error))?;
    let value = ctx
        .backend
        .push_file(&ctx.table, name, storage, image)
        .map_err(|detail| ctx.error.error(detail))?;
    Ok(ColumnValue::File(value))
}

async fn process_file_field<'source, R: RecordBackend + Send + Sync>(
    ctx: &RecordContext<'source, R>,
    name: &str,
    storage: &config::FileStorage,
    value: serde_json::Value,
) -> Result<ColumnValue, Error> {
    if let serde_json::Value::String(src) = value {
        process_file_field_impl(ctx, name, storage, &src).await
    } else {
        bail!(
            ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "string",
                got: value
            }
        )
    }
}

async fn process_markdown_field_impl<'source, R: RecordBackend + Send + Sync>(
    ctx: &RecordContext<'source, R>,
    id: &CompoundId,
    storage: &config::MarkdownStorage,
    _: &config::MarkdownConfig,
    image: &config::MarkdownImageConfig,
    src: &str,
) -> Result<ColumnValue, Error> {
    let document = markdown::parser::parse(src);
    let document = markdown::resolver::RichTextDocument::resolve(
        document,
        Some(&ctx.document_path),
        ctx.backend,
        &ctx.table,
        &image.transform,
        &image.storage,
        image.embed_svg_threshold,
    )
    .await
    .map_err(|detail| ctx.error.error(detail))?;
    let document = markdown::compress::compress(document);
    match storage {
        MarkdownStorage::Inline => Ok(ColumnValue::Markdown(MarkdownReference::Inline {
            content: document,
        })),
        MarkdownStorage::Kv { namespace, prefix } => {
            match ctx
                .backend
                .push_markdown(
                    id,
                    &backend::MarkdownStorage::Kv {
                        namespace: namespace.clone(),
                        prefix: prefix.clone(),
                    },
                    document,
                )
                .map_err(|detail| ctx.error.error(detail))?
            {
                backend::MarkdownReference::Kv { key } => {
                    Ok(ColumnValue::Markdown(MarkdownReference::Kv { key }))
                }
            }
        }
    }
}

async fn process_markdown_field<'source, R: RecordBackend + Send + Sync>(
    ctx: &RecordContext<'source, R>,
    id: &CompoundId,
    storage: &config::MarkdownStorage,
    config: &config::MarkdownConfig,
    image: &config::MarkdownImageConfig,
    value: serde_json::Value,
) -> Result<ColumnValue, Error> {
    if let serde_json::Value::String(src) = value {
        process_markdown_field_impl(ctx, id, storage, config, image, &src).await
    } else {
        bail!(
            ctx.error,
            ErrorDetail::TypeMismatch {
                expected: "string",
                got: value
            }
        )
    }
}

async fn process_field<'source, R: RecordBackend + Sync + Send>(
    ctx: &RecordContext<'source, R>,
    id: &CompoundId,
    name: &str,
    def: &schema::FieldType,
    value: Option<serde_json::Value>,
) -> Result<Option<ColumnValue>, Error> {
    let value = match value {
        Some(value) => value,
        None => {
            if is_normal_required_field(def) {
                bail!(&ctx.error, ErrorDetail::MissingField(name.to_owned()));
            } else {
                return Ok(Some(ColumnValue::Null));
            }
        }
    };
    match def {
        schema::FieldType::Id => unreachable!(),
        schema::FieldType::Hash => process_hash_field(ctx, name).map(Some),
        schema::FieldType::Boolean { .. } => process_boolean_field(ctx, value).map(Some),
        schema::FieldType::String { .. } => process_string_field(ctx, value).map(Some),
        schema::FieldType::Integer { .. } => process_integer_field(ctx, value).map(Some),
        schema::FieldType::Real { .. } => process_real_field(ctx, value).map(Some),
        schema::FieldType::Date { .. } => process_date_field(ctx, value).map(Some),
        schema::FieldType::Datetime { .. } => process_datetime_field(ctx, value).map(Some),
        schema::FieldType::Image {
            transform, storage, ..
        } => process_image_field(ctx, name, transform, storage, value)
            .await
            .map(Some),
        schema::FieldType::File { storage, .. } => process_file_field(ctx, name, storage, value)
            .await
            .map(Some),
        schema::FieldType::Markdown {
            image,
            config,
            storage,
            ..
        } => process_markdown_field(ctx, id, storage, config, image, value)
            .await
            .map(Some),
        schema::FieldType::Records { table, .. } => {
            process_records_field(ctx, id, table, value).await?;
            Ok(None)
        }
    }
}

async fn push_rows_impl<'source, R: RecordBackend + Sync + Send>(
    ctx: &RecordContext<'source, R>,
    mut fields: serde_json::Map<String, serde_json::Value>,
) -> Result<(), Error> {
    let schema = ctx.current_schema();
    let id =
        extract_id_value(&schema.id_name, &mut fields).map_err(|detail| ctx.error.error(detail))?;
    let id = ctx.id(id);

    let ctx = RecordContext {
        error: ctx.error.with_id(id.clone()),
        ..ctx.clone()
    };

    let mut row = IndexMap::new();
    for (name, def) in &schema.fields {
        row.insert(
            name.clone(),
            process_field(&ctx, &id, name, def, fields.remove(name))
                .await?
                .unwrap_or(ColumnValue::Null),
        );
    }
    ctx.backend.push_row(&ctx.table, row);
    Ok(())
}
fn push_rows<'source, 'c, R: RecordBackend + Sync + Send>(
    ctx: &'c RecordContext<'source, R>,
    fields: serde_json::Map<String, serde_json::Value>,
) -> Pin<Box<dyn 'c + Future<Output = Result<(), Error>>>> {
    Box::pin(push_rows_impl(ctx, fields))
}

pub async fn push_rows_from_document<P: AsRef<Path>, R: RecordBackend + Sync + Send>(
    table: &str,
    schema: &schema::TableSchemas,
    syntax: &DocumentSyntax,
    backend: &R,
    path: P,
) -> Result<(), Error> {
    let ctx = ErrorContext::new(path.as_ref().to_owned());
    let mut hasher = blake3::Hasher::new();
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
        table: table.to_owned(),
        schema: Arc::new(schema.clone()),
        compound_id_prefix: Default::default(),
        error: ctx,
        document_path: path.as_ref().to_owned(),
        backend,
    };
    push_rows(&ctx, fields).await?;
    Ok(())
}
