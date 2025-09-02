use std::{
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, LazyLock},
};

use futures::future::try_join_all;
use indexmap::{IndexMap, indexmap};
use serde::{
    Serialize,
    ser::{SerializeMap as _, SerializeSeq, SerializeTuple},
};

use crate::{
    Error, ErrorContext, ErrorDetail, backend,
    config::{self, DocumentSyntax},
    field::{
        self, ColumnValue, CompoundId, CompoundIdPrefix,
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

struct RowNode {
    id: CompoundId,
    fields: IndexMap<String, ColumnValue>,
    records: IndexMap<String, Records>,
    hash: blake3::Hash,
}

struct Records {
    table: String,
    rows: Vec<RowNode>,
}

impl Serialize for Records {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut serializer = serializer.serialize_seq(Some(self.rows.len()))?;
        for row in &self.rows {
            serializer.serialize_element(&row.fields)?;
        }
        serializer.end()
    }
}

enum FieldValue {
    Column(ColumnValue),
    Markdown {
        document: compress::RichTextDocument,
        storage: config::MarkdownStorage,
    },
    Records(Records),
}

impl Serialize for FieldValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Column(column) => column.serialize(serializer),
            Self::Records(records) => {
                let mut serializer = serializer.serialize_tuple(records.rows.len())?;
                for row in &records.rows {
                    serializer.serialize_element(&row.fields)?;
                }
                serializer.end()
            }
            Self::Markdown { .. } => unreachable!("Markdown cannot serialize"),
        }
    }
}

struct RecordContext<'c> {
    table: String,
    schema: Arc<TableSchemas>,
    hasher: blake3::Hasher,
    compound_id_prefix: CompoundIdPrefix,
    error: ErrorContext,
    document_path: PathBuf,
    backend: &'c backend::Uploads,
}

impl<'c> Clone for RecordContext<'c> {
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

impl<'source> RecordContext<'source> {
    fn current_schema(&self) -> &Arc<schema::Schema> {
        self.schema.get(&self.table).unwrap()
    }

    fn nest(self, table: impl Into<String>, id: CompoundId) -> Result<Self, crate::Error> {
        let table = table.into();
        let inherit_ids = self.schema.get(&table).unwrap().inherit_ids.clone();
        let Self {
            schema,
            error,
            document_path,
            backend,
            ..
        } = self;
        let compound_id_prefix = id
            .try_into_prefix(inherit_ids)
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

fn process_hash_field<'source>(
    ctx: &RecordContext<'source>,
    name: &str,
) -> Result<ColumnValue, Error> {
    bail!(ctx.error, ErrorDetail::FoundComputedField(name.to_owned()))
}

fn process_boolean_field<'source>(
    ctx: &RecordContext<'source>,
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

fn process_integer_field<'source>(
    ctx: &RecordContext<'source>,
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

fn process_real_field<'source>(
    ctx: &RecordContext<'source>,
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

fn process_string_field<'source>(
    ctx: &RecordContext<'source>,
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

fn process_date_field<'source>(
    ctx: &RecordContext<'source>,
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

fn process_datetime_field<'source>(
    ctx: &RecordContext<'source>,
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

async fn process_records_field<'source>(
    ctx: &RecordContext<'source>,
    id: &CompoundId,
    table: &str,
    value: serde_json::Value,
) -> Result<Vec<RowNode>, Error> {
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
                    let fields = indexmap! {
                        ctx.current_schema().id_name.clone() => ColumnValue::Id(id.clone()),
                    };
                    let id = ctx.id(id);
                    Ok(RowNode {
                        id,
                        hash: ctx.hasher.finalize(),
                        fields,
                        records: Default::default(),
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

async fn process_image_field<'source>(
    ctx: &RecordContext<'source>,
    hasher: &mut blake3::Hasher,
    id: &CompoundId,
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
    let reference = ctx.backend.push_image(storage, id, image.clone(), false);
    Ok(ColumnValue::Image(reference))
}

async fn process_file_field<'source>(
    ctx: &RecordContext<'source>,
    hasher: &mut blake3::Hasher,
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
    let reference = ctx.backend.push_file(storage, id, file.clone());
    Ok(ColumnValue::File(reference))
}

async fn process_markdown_field<'source>(
    ctx: &RecordContext<'source>,
    hasher: &mut blake3::Hasher,
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
        id,
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

async fn process_field<'source>(
    ctx: &RecordContext<'source>,
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
        schema::FieldType::Image { storage, .. } => {
            process_image_field(ctx, hasher, id, storage, value)
                .await
                .map(FieldValue::Column)?
        }
        schema::FieldType::File { storage, .. } => {
            process_file_field(ctx, hasher, id, storage, value)
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
                process_markdown_field(ctx, hasher, id, storage, config, image, value).await?;
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

struct Frontmatter<'a> {
    fields: &'a IndexMap<String, ColumnValue>,
    records: &'a IndexMap<String, Records>,
}

impl<'a> Serialize for Frontmatter<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut serializer =
            serializer.serialize_map(Some(self.fields.len() + self.records.len()))?;
        for (name, value) in self.fields {
            serializer.serialize_entry(name, value)?;
        }
        for (name, records) in self.records {
            serializer.serialize_entry(name, records)?;
        }
        serializer.end()
    }
}

async fn process_row_impl<'source>(
    ctx: &RecordContext<'source>,
    mut raw_fields: serde_json::Map<String, serde_json::Value>,
) -> Result<RowNode, Error> {
    let schema = ctx.current_schema();
    let id = extract_id_value(&schema.id_name, &mut raw_fields)
        .map_err(|detail| ctx.error.error(detail))?;
    let id = ctx.id(id);

    let ctx = RecordContext {
        error: ctx.error.with_id(id.clone()),
        ..ctx.clone()
    };

    let mut hasher = ctx.hasher.clone();

    let mut fields = IndexMap::new();

    for (name, id) in id.pairs() {
        fields.insert(name.into(), ColumnValue::Id(id.into()));
    }

    let mut records = IndexMap::new();
    let mut markdowns = IndexMap::new();

    for (name, def) in &schema.fields {
        match process_field(&ctx, &mut hasher, &id, name, def, raw_fields.remove(name)).await? {
            Some(FieldValue::Column(value)) => {
                fields.insert(name.clone(), value);
            }
            Some(FieldValue::Records(value)) => {
                records.insert(name.clone(), value);
            }
            Some(FieldValue::Markdown {
                document,
                storage: config::MarkdownStorage::Inline,
            }) => {
                fields.insert(
                    name.clone(),
                    ColumnValue::Markdown(field::MarkdownReference::Inline {
                        content: serde_json::to_value(&document).unwrap(),
                    }),
                );
            }
            Some(FieldValue::Markdown {
                document,
                storage: config::MarkdownStorage::Kv { namespace, prefix },
            }) => {
                markdowns.insert(
                    name.clone(),
                    (document, backend::MarkdownStorage::Kv { namespace, prefix }),
                );
            }
            None => {}
        }
    }
    let hash = hasher.finalize();
    if let Some(hash_name) = &schema.hash_name {
        fields.insert(hash_name.clone(), ColumnValue::Hash(hash));
    }

    let frontmatter = Frontmatter {
        fields: &fields,
        records: &records,
    };
    let frontmatter = serde_json::to_value(&frontmatter).unwrap();
    for (document, storage) in markdowns.into_values() {
        ctx.backend
            .push_markdown(&storage, &id, document, frontmatter.clone());
    }
    Ok(RowNode {
        id,
        fields,
        hash: hasher.finalize(),
        records,
    })
}

fn process_row<'source, 'c>(
    ctx: &'c RecordContext<'source>,
    fields: serde_json::Map<String, serde_json::Value>,
) -> Pin<Box<dyn 'c + Future<Output = Result<RowNode, Error>>>> {
    Box::pin(process_row_impl(ctx, fields))
}

pub type Tables = IndexMap<String, Vec<IndexMap<String, ColumnValue>>>;

fn tree_to_flat_tables(
    schema: &schema::TableSchemas,
    tables: &mut Tables,
    table: String,
    row: RowNode,
) {
    let mut fields = row.fields;
    for (name, id) in row.id.pairs() {
        fields.insert(name.into(), ColumnValue::Id(id.into()));
    }
    if let Some(hash_name) = schema
        .get(&table)
        .and_then(|table| table.hash_name.as_ref())
    {
        fields.insert(hash_name.clone(), ColumnValue::Hash(row.hash));
    }
    tables.entry(table).or_default().push(fields);
    for (_, record) in row.records {
        record.rows.into_iter().for_each(|row| {
            tree_to_flat_tables(schema, tables, record.table.clone(), row);
        });
    }
}

pub async fn push_rows_from_document<P: AsRef<Path>>(
    table: &str,
    mut hasher: blake3::Hasher,
    schema: &schema::TableSchemas,
    syntax: &DocumentSyntax,
    backend: &backend::Uploads,
    path: P,
) -> Result<Tables, Error> {
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

    let mut tables = IndexMap::new();
    tree_to_flat_tables(
        schema,
        &mut tables,
        table.into(),
        process_row(&ctx, fields).await?,
    );

    Ok(tables)
}
