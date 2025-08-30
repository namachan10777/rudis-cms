use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
};

use futures::future::try_join_all;
use image::DynamicImage;
use indexmap::IndexMap;

use crate::{
    backend::{self, RecordBackend},
    config::{self, DocumentSyntax},
    field::object_loader,
    schema::{self, TableSchemas},
};

pub struct ImageColumn {}

pub struct FileColumn {}

pub trait Uploader {
    fn upload_image(&self, image: object_loader::Image) -> Result<ImageColumn, Error>;
    fn upload_file(&self, file: object_loader::Object) -> Result<FileColumn, Error>;
}

pub enum Scalar {
    Null,
    String(String),
    Number(serde_json::Number),
    Boolean(bool),
    Object(serde_json::Map<String, serde_json::Value>),
    Date(chrono::NaiveDate),
    Datetime(chrono::NaiveDateTime),
    Array(Vec<serde_json::Value>),
}

impl From<serde_json::Value> for Scalar {
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(b) => Self::Boolean(b),
            serde_json::Value::Number(n) => Self::Number(n),
            serde_json::Value::String(s) => Self::String(s),
            serde_json::Value::Array(arr) => Self::Array(arr),
            serde_json::Value::Object(obj) => Self::Object(obj),
        }
    }
}

impl From<String> for Scalar {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

#[derive(Default)]
struct Row {
    fields: IndexMap<String, Scalar>,
}

impl Row {
    fn id_only(name: impl Into<String>, value: impl Into<String>) -> Self {
        let mut fields = IndexMap::new();
        let value = value.into();
        fields.insert(name.into(), value.into());
        Self { fields }
    }

    fn with_compount_id(self, id: &CompoundId) -> Self {
        let mut fields = IndexMap::with_capacity(self.fields.len() + id.prefix.0.len() + 1);
        for (key, value) in &id.prefix.0 {
            fields.insert(key.clone(), value.clone().into());
        }
        fields.insert(id.name.clone(), id.id.clone().into());
        for (key, value) in self.fields {
            fields.insert(key, value);
        }
        Self { fields }
    }
}

#[derive(Debug, Clone)]
pub struct ErrorContext {
    pub path: PathBuf,
    pub id: Option<CompoundId>,
}

impl ErrorContext {
    fn new(path: PathBuf) -> Self {
        Self { path, id: None }
    }

    fn new_with_id(path: PathBuf, id: CompoundId) -> Self {
        Self { path, id: Some(id) }
    }

    fn with_id(&self, id: CompoundId) -> Self {
        Self {
            path: self.path.clone(),
            id: Some(id),
        }
    }

    fn error(&self, detail: ErrorDetail) -> Error {
        Error {
            context: self.clone(),
            detail,
        }
    }
}

impl std::fmt::Display for ErrorContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.id {
            Some(id) => write!(f, "{id}({})", self.path.display()),
            None => write!(f, "{}", self.path.display()),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{context}: {detail}")]
pub struct Error {
    pub context: ErrorContext,
    pub detail: ErrorDetail,
}

#[derive(Debug, thiserror::Error)]
pub enum ErrorDetail {
    #[error("Failed to read document: {0}")]
    ReadDocument(std::io::Error),
    #[error("Failed to parse TOML document: {0}")]
    ParseToml(toml::de::Error),
    #[error("Failed to parse YAML document: {0}")]
    ParseYaml(serde_yaml::Error),
    #[error("Unclosed frontmatter")]
    UnclosedFrontmatter,
    #[error("Type mismatch: expected {expected}, got {got}")]
    TypeMismatch {
        expected: &'static str,
        got: serde_json::Value,
    },
    #[error("Missing field: {0}")]
    MissingField(String),
    #[error("Invalid date: {0}")]
    InvalidDate(String),
    #[error("Invalid datetime: {0}")]
    InvalidDatetime(String),
    #[error("Found computed field: {0}")]
    FoundComputedField(String),
    #[error("Failed to load image: {0}")]
    LoadImage(object_loader::ImageLoadError),
    #[error("Failed to load: {0}")]
    Load(object_loader::Error),
}

enum Generated {
    R2ImageUpload {
        bucket: String,
        key: String,
        width: u32,
        height: u32,
        format: config::ImageFormat,
        data: Arc<DynamicImage>,
    },
    R2ObjectUpload {
        bucket: String,
        key: String,
        content_type: String,
        data: Arc<Box<u8>>,
    },
    D1Row {
        table: String,
        row: Row,
    },
}

#[derive(Clone)]
pub(crate) struct RowSink<'s> {
    tx: smol::channel::Sender<Generated>,
    _marker: PhantomData<&'s ()>,
}

#[derive(Clone)]
pub(crate) struct RowSource {
    rx: smol::channel::Receiver<Generated>,
    tx: smol::channel::Sender<Generated>,
}

impl Default for RowSource {
    fn default() -> Self {
        let (tx, rx) = smol::channel::unbounded();
        Self { rx, tx }
    }
}

impl RowSource {
    pub(crate) fn sink(&self) -> RowSink<'_> {
        RowSink {
            tx: self.tx.clone(),
            _marker: Default::default(),
        }
    }
}

impl<'source> RowSink<'source> {
    async fn push_d1_row(&self, table: &str, row: Row) {
        self.tx
            .send(Generated::D1Row {
                table: table.to_owned(),
                row,
            })
            .await
            .expect("invariant is broken");
    }
}

#[derive(Clone, Default)]
struct CompoundIdPrefix(Vec<(String, String)>);

#[derive(Clone)]
pub struct CompoundId {
    prefix: CompoundIdPrefix,
    id: String,
    name: String,
}

impl std::fmt::Display for CompoundId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (_, id) in &self.prefix.0 {
            write!(f, "{id}/")?;
        }
        f.write_str(&self.id)
    }
}

impl std::fmt::Debug for CompoundId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
    }
}

impl CompoundId {
    fn into_prefix(self) -> CompoundIdPrefix {
        let Self {
            id,
            name,
            mut prefix,
        } = self;
        prefix.0.push((name, id));
        prefix
    }
}

impl CompoundIdPrefix {
    fn id(self, name: impl Into<String>, id: impl Into<String>) -> CompoundId {
        CompoundId {
            prefix: self,
            id: id.into(),
            name: name.into(),
        }
    }
}

static FRONTMATTER_SEPARATOR_YAML: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"(?:^|\n)---\s*\n").unwrap());

static FRONTMATTER_SEPARATOR_TOML: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"(?:^|\n)\+\+\+\s*\n").unwrap());

static WHITESPACE: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"^\s*$").unwrap());

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

struct FieldPacker<'s> {
    schema: &'s IndexMap<String, config::Field>,
    records: IndexMap<String, Row>,
    hashes: smol::lock::Mutex<Vec<blake3::Hash>>,
    ids: Vec<String>,
}

struct PackedTable {
    name: String,
    rows: Vec<Row>,
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
    sink: RowSink<'c>,
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
            sink: self.sink.clone(),
            backend: self.backend,
        }
    }
}

impl<'source, R> RecordContext<'source, R> {
    fn current_schema(&self) -> &Arc<schema::Schema> {
        self.schema.get(&self.table).unwrap()
    }

    fn nest(self, table: impl Into<String>, id: CompoundId) -> Self {
        let Self {
            schema,
            error,
            sink,
            document_path,
            backend,
            ..
        } = self;
        let compound_id_prefix = id.into_prefix();
        Self {
            table: table.into(),
            schema,
            compound_id_prefix,
            error,
            sink,
            document_path,
            backend,
        }
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

fn process_hash_field<'source, R>(
    ctx: &RecordContext<'source, R>,
    name: &str,
) -> Result<Scalar, Error> {
    bail!(ctx.error, ErrorDetail::FoundComputedField(name.to_owned()))
}

fn process_boolean_field<'source, R>(
    ctx: &RecordContext<'source, R>,
    value: serde_json::Value,
) -> Result<Scalar, Error> {
    if let serde_json::Value::Bool(b) = value {
        Ok(Scalar::Boolean(b))
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

fn process_integer_field<'source, R>(
    ctx: &RecordContext<'source, R>,
    value: serde_json::Value,
) -> Result<Scalar, Error> {
    if let serde_json::Value::Number(n) = value {
        if n.is_i64() {
            Ok(Scalar::Number(n))
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

fn process_real_field<'source, R>(
    ctx: &RecordContext<'source, R>,
    value: serde_json::Value,
) -> Result<Scalar, Error> {
    if let serde_json::Value::Number(n) = value {
        if n.is_f64() {
            Ok(Scalar::Number(n))
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

fn process_string_field<'source, R>(
    ctx: &RecordContext<'source, R>,
    value: serde_json::Value,
) -> Result<Scalar, Error> {
    if let serde_json::Value::String(string) = value {
        Ok(Scalar::String(string))
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

fn process_date_field<'source, R>(
    ctx: &RecordContext<'source, R>,
    value: serde_json::Value,
) -> Result<Scalar, Error> {
    if let serde_json::Value::String(date) = value {
        let date = date
            .parse::<chrono::NaiveDate>()
            .map_err(|_| ctx.error.error(ErrorDetail::InvalidDate(date.to_owned())))?;
        Ok(Scalar::Date(date))
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

fn process_datetime_field<'source, R>(
    ctx: &RecordContext<'source, R>,
    value: serde_json::Value,
) -> Result<Scalar, Error> {
    if let serde_json::Value::String(datetime) = value {
        let datetime = datetime.parse::<chrono::NaiveDateTime>().map_err(|_| {
            ctx.error
                .error(ErrorDetail::InvalidDatetime(datetime.to_owned()))
        })?;
        Ok(Scalar::Datetime(datetime))
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

async fn process_records_field_impl<'source, R: RecordBackend + Sync>(
    ctx: &RecordContext<'source, R>,
    id: &CompoundId,
    table: &str,
    records: Vec<serde_json::Value>,
) -> Result<(), Error> {
    let ctx = ctx.clone().nest(table, id.clone());
    let tasks = records.into_iter().map(|record| async {
        match record {
            serde_json::Value::String(id) => {
                if ctx.current_schema().is_id_only_table() {
                    let id = ctx.id(id);
                    let row = Row::default().with_compount_id(&id);
                    ctx.sink.push_d1_row(&table, row).await;
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

async fn process_records_field<'source, R: RecordBackend + Sync>(
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
async fn process_image_field_impl<'source, R: RecordBackend>(
    ctx: &RecordContext<'source, R>,
    id: &CompoundId,
    transform: &config::ImageTransform,
    src: &str,
) -> Result<Scalar, Error> {
    let image = object_loader::load_image(&src, Some(&ctx.document_path))
        .await
        .map_err(ErrorDetail::LoadImage)
        .map_err(|error| ctx.error.error(error))?;
    match image.body {
        object_loader::ImageContent::Raster { data } => {
            let raster_image = backend::RasterImage {
                data: data,
                hash: image.hash,
                origin: image.origin,
                derived_id: image.derived_id,
            };
            let locator = ctx.backend.raster_image_locator(&id, &raster_image);
            unimplemented!()
        }
        object_loader::ImageContent::Vector { dimensions, tree } => {
            unimplemented!()
        }
    }
}

async fn process_image_field<'source, R: RecordBackend>(
    ctx: &RecordContext<'source, R>,
    id: &CompoundId,
    transform: &config::ImageTransform,
    value: serde_json::Value,
) -> Result<Scalar, Error> {
    if let serde_json::Value::String(src) = value {
        process_image_field_impl(ctx, id, transform, &src).await
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

async fn process_file_field_impl<'source, R: RecordBackend>(
    ctx: &RecordContext<'source, R>,
    id: &CompoundId,
    src: &str,
) -> Result<Scalar, Error> {
    unimplemented!()
}

async fn process_file_field<'source, R: RecordBackend>(
    ctx: &RecordContext<'source, R>,
    id: &CompoundId,
    value: serde_json::Value,
) -> Result<Scalar, Error> {
    if let serde_json::Value::String(src) = value {
        process_file_field_impl(ctx, id, &src).await
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

async fn process_markdown_field_impl<'source, R: RecordBackend>(
    ctx: &RecordContext<'source, R>,
    id: &CompoundId,
    config: &config::MarkdownConfig,
    image: &config::MarkdownImageConfig,
    src: &str,
) -> Result<Scalar, Error> {
    unimplemented!()
}

async fn process_markdown_field<'source, R: RecordBackend>(
    ctx: &RecordContext<'source, R>,
    id: &CompoundId,
    config: &config::MarkdownConfig,
    image: &config::MarkdownImageConfig,
    value: serde_json::Value,
) -> Result<Scalar, Error> {
    if let serde_json::Value::String(src) = value {
        process_markdown_field_impl(ctx, id, config, image, &src).await
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

async fn process_field<'source, R: RecordBackend + Sync>(
    ctx: &RecordContext<'source, R>,
    id: &CompoundId,
    name: &str,
    def: &schema::FieldType,
    value: Option<serde_json::Value>,
) -> Result<Option<Scalar>, Error> {
    let value = match value {
        Some(value) => value,
        None => {
            if is_normal_required_field(def) {
                bail!(&ctx.error, ErrorDetail::MissingField(name.to_owned()));
            } else {
                return Ok(Some(Scalar::Null));
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
        schema::FieldType::Image { transform, .. } => {
            process_image_field(ctx, id, transform, value)
                .await
                .map(Some)
        }
        schema::FieldType::File { .. } => process_file_field(ctx, id, value).await.map(Some),
        schema::FieldType::Markdown { image, config, .. } => {
            process_markdown_field(ctx, id, config, image, value)
                .await
                .map(Some)
        }
        schema::FieldType::Records { table, .. } => {
            process_records_field(ctx, id, table, value).await?;
            Ok(None)
        }
    }
}

#[async_recursion::async_recursion]
async fn push_rows<'source, R: RecordBackend + Sync>(
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
                .unwrap_or(Scalar::Null),
        );
    }
    let row = Row { fields: row };
    ctx.sink.push_d1_row(&ctx.table, row).await;
    Ok(())
}

pub(crate) struct Context {
    table: String,
    schema: schema::TableSchemas,
}

pub(crate) async fn push_rows_from_document<P: AsRef<Path>, R: RecordBackend + Sync>(
    sink: RowSink<'_>,
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
        sink,
        document_path: path.as_ref().to_owned(),
        backend,
    };
    push_rows(&ctx, fields).await?;
    Ok(())
}
