//! Data transformation and field processing
//!
//! This module handles transformation of raw field values into typed column values,
//! including async processing for images, files, and markdown content.

use std::pin::Pin;

use futures::future::try_join_all;
use indexmap::{IndexMap, indexmap};
use tracing::{debug, trace};
use valuable::Valuable;

use crate::{
    Error, ErrorDetail, config,
    process_data::{
        ColumnValue, CompoundId, ImageReferenceMeta, ObjectReference, StorageContent,
        StorageContentRef, markdown, object_loader,
    },
    schema,
};

use super::{
    context::RecordContext,
    types::{FieldValue, Records, RowNode, Upload},
    validate::{
        is_normal_required_field, process_boolean_field, process_date_field,
        process_datetime_field, process_hash_field, process_integer_field, process_real_field,
        process_string_field,
    },
};

macro_rules! bail {
    ($ctx:expr, $detail:expr) => {
        return Err($ctx.error($detail))
    };
}

/// Process a records field (nested table).
pub async fn process_records_field(
    ctx: &RecordContext,
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
                        uploads: Default::default(),
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

/// Process an image field.
pub async fn process_image_field(
    ctx: &RecordContext,
    id: &CompoundId,
    storage: &config::Storage,
    value: serde_json::Value,
) -> Result<FieldValue, Error> {
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
    let (width, height) = image.body.dimensions();
    let meta = ImageReferenceMeta {
        width,
        height,
        derived_id: image.derived_id,
        blurhash: None, // TODO
    };
    let reference = ObjectReference::build(
        StorageContentRef::Bytes(&image.original),
        id,
        image.content_type.clone(),
        meta,
        storage,
        None,
    );
    let upload = Upload {
        data: StorageContent::Bytes(image.original.into_vec()),
        hash: reference.hash,
        pointer: reference.pointer.clone(),
        content_type: image.content_type,
        source_entry: None,
    };
    Ok(FieldValue::WithUpload {
        column: ColumnValue::Image(reference),
        upload,
    })
}

/// Process a file field.
pub async fn process_file_field(
    ctx: &RecordContext,
    hasher: &mut blake3::Hasher,
    id: &CompoundId,
    storage: &config::Storage,
    value: serde_json::Value,
) -> Result<FieldValue, Error> {
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
    let reference = ObjectReference::build(
        StorageContentRef::Bytes(&file.body),
        id,
        file.content_type.clone(),
        (),
        storage,
        None,
    );
    Ok(FieldValue::WithUpload {
        upload: Upload {
            data: StorageContent::Bytes(file.body.into_vec()),
            hash: reference.hash,
            pointer: reference.pointer.clone(),
            content_type: file.content_type,
            source_entry: None,
        },
        column: ColumnValue::File(reference),
    })
}

struct MarkdownImageUploader<'a> {
    storage: &'a config::Storage,
    queue: crossbeam::queue::SegQueue<(ObjectReference<ImageReferenceMeta>, Vec<u8>)>,
    id: &'a CompoundId,
}

impl<'a> markdown::resolver::ImageUploadRegisterer for MarkdownImageUploader<'a> {
    fn register(&self, image: object_loader::Image) -> ObjectReference<ImageReferenceMeta> {
        let (width, height) = image.body.dimensions();
        let meta = ImageReferenceMeta {
            width,
            height,
            derived_id: image.derived_id.clone(),
            blurhash: None, // TODO
        };
        let reference = ObjectReference::build(
            StorageContentRef::Bytes(&image.original),
            self.id,
            image.content_type,
            meta,
            self.storage,
            Some(image.derived_id),
        );
        self.queue
            .push((reference.clone(), image.original.into_vec()));
        reference
    }
}

/// Process a markdown field.
pub async fn process_markdown_field(
    ctx: &RecordContext,
    hasher: &mut blake3::Hasher,
    id: &CompoundId,
    storage: &config::Storage,
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
    let image_uploader = MarkdownImageUploader {
        storage: &image.storage,
        queue: Default::default(),
        id,
    };
    let (document, hashes) = markdown::resolver::RichTextDocument::resolve(
        document,
        Some(&ctx.document_path),
        &image_uploader,
        image.embed_svg_threshold,
    )
    .await
    .map_err(|detail| ctx.error.error(detail))?;
    let document = markdown::compress::compress(document);
    hashes.iter().for_each(|hash| {
        hasher.update(hash.as_bytes());
    });

    trace!(
        table = image.table,
        prefix = ctx.compound_id_prefix.as_value(),
        id = id.as_value(),
        "enter markdown image table"
    );
    let ctx = ctx.clone().nest(&image.table, id.clone())?;

    let value = FieldValue::Markdown {
        document,
        image_table: image.table.clone(),
        image_rows: image_uploader
            .queue
            .into_iter()
            .map(|(reference, data)| {
                debug!(
                    markdown_id = id.as_value(),
                    id = ctx.id(&reference.meta.derived_id).as_value(),
                    "markdown image"
                );
                RowNode {
                    id: ctx.id(&reference.meta.derived_id),
                    hash: reference.hash,
                    fields: indexmap! {
                        "image".to_string() => ColumnValue::Image(reference.clone())
                    },
                    records: Default::default(),
                    uploads: vec![Upload {
                        data: StorageContent::Bytes(data),
                        hash: reference.hash,
                        pointer: reference.pointer,
                        content_type: reference.content_type,
                        source_entry: None,
                    }],
                }
            })
            .collect(),
        storage: storage.clone(),
    };
    Ok((value, hasher.finalize()))
}

/// Process a single field based on its type.
pub async fn process_field(
    ctx: &RecordContext,
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
            process_image_field(ctx, id, storage, value).await?
        }
        schema::FieldType::File { storage, .. } => {
            process_file_field(ctx, hasher, id, storage, value).await?
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

/// Process a single row of data.
async fn process_row_impl(
    ctx: &RecordContext,
    mut raw_fields: serde_json::Map<String, serde_json::Value>,
) -> Result<RowNode, Error> {
    use super::parse::extract_id_value;
    use super::serialize::Frontmatter;

    let schema = ctx.current_schema();
    let id = extract_id_value(&schema.id_name, &mut raw_fields)
        .map_err(|detail| ctx.error.error(detail))?;
    let id = ctx.id(id);

    let ctx = ctx.with_error_id(id.clone());

    let mut hasher = ctx.hasher.clone();

    let mut fields = IndexMap::new();

    for (name, id) in id.pairs() {
        fields.insert(name.into(), ColumnValue::Id(id.into()));
    }

    let mut records = IndexMap::new();
    let mut markdowns = IndexMap::new();
    let mut total_uploads = Vec::new();

    for (name, def) in &schema.fields {
        match process_field(&ctx, &mut hasher, &id, name, def, raw_fields.remove(name)).await? {
            Some(FieldValue::Column(value)) => {
                fields.insert(name.clone(), value);
            }
            Some(FieldValue::WithUpload { column, upload }) => {
                fields.insert(name.clone(), column);
                total_uploads.push(upload);
            }
            Some(FieldValue::Records(value)) => {
                records.insert(name.clone(), value);
            }
            Some(FieldValue::Markdown {
                document,
                image_table,
                mut image_rows,
                storage: config::Storage::Inline,
            }) => {
                let content = serde_json::to_string(&document).unwrap();
                records
                    .entry(image_table.clone())
                    .or_insert_with(|| Records {
                        table: image_table.clone(),
                        rows: Default::default(),
                    })
                    .rows
                    .append(&mut image_rows);
                fields.insert(
                    name.clone(),
                    ColumnValue::Markdown(ObjectReference::build(
                        StorageContentRef::Text(&content),
                        &id,
                        "application/json".into(),
                        (),
                        &config::Storage::Inline,
                        None,
                    )),
                );
            }
            Some(FieldValue::Markdown {
                document,
                image_table,
                mut image_rows,
                storage,
            }) => {
                records
                    .entry(image_table.clone())
                    .or_insert_with(|| Records {
                        table: image_table.clone(),
                        rows: Default::default(),
                    })
                    .rows
                    .append(&mut image_rows);
                markdowns.insert(name.clone(), (document, storage));
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
    for (name, (document, storage)) in markdowns.into_iter() {
        let content = serde_json::to_string(&serde_json::json!({
            "frontmatter": &frontmatter,
            "root": document.root,
            "footnotes": document.footnotes,
            "sections": document.sections
        }))
        .unwrap();
        let reference = ObjectReference::build(
            StorageContentRef::Text(&content),
            &id,
            "application/json".into(),
            (),
            &storage,
            None,
        );
        fields.insert(name, ColumnValue::Markdown(reference.clone()));
        total_uploads.push(Upload {
            data: StorageContent::Text(content),
            hash: reference.hash,
            pointer: reference.pointer,
            content_type: reference.content_type,
            source_entry: None,
        });
    }
    Ok(RowNode {
        id,
        fields,
        hash: hasher.finalize(),
        records,
        uploads: total_uploads,
    })
}

/// Process a row (wrapper for async recursion).
pub fn process_row<'c>(
    ctx: &'c RecordContext,
    fields: serde_json::Map<String, serde_json::Value>,
) -> Pin<Box<dyn 'c + std::future::Future<Output = Result<RowNode, Error>>>> {
    Box::pin(process_row_impl(ctx, fields))
}
