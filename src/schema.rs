use std::sync::Arc;

use indexmap::IndexMap;

use crate::config;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Id field is undefined")]
    IdUndefined,
    #[error("Hash field is undefined")]
    HashUndefined,
}

pub struct Schema {
    pub(crate) fields: IndexMap<String, FieldType>,
    pub(crate) compound_id_prefix_names: Vec<String>,
    pub(crate) id_name: String,
    pub(crate) hash_name: Option<String>,
}

pub(crate) type TableSchemas = IndexMap<String, Arc<Schema>>;

pub(crate) enum FieldType {
    Id,
    Hash,
    String {
        required: bool,
        index: bool,
    },
    Integer {
        required: bool,
        index: bool,
    },
    Real {
        required: bool,
        index: bool,
    },
    Boolean {
        required: bool,
        index: bool,
    },
    Date {
        required: bool,
        index: bool,
    },
    Datetime {
        required: bool,
        index: bool,
    },
    Image {
        index: bool,
        required: bool,
        storage: config::ImageStorage,
        transform: config::ImageTransform,
    },
    File {
        index: bool,
        required: bool,
        storage: config::FileStorage,
    },
    Markdown {
        required: bool,
        image: config::MarkdownImageConfig,
        config: config::MarkdownConfig,
        storage: config::MarkdownStorage,
    },
    Records {
        table: String,
        required: bool,
    },
}

impl FieldType {}

impl Schema {
    fn add_table(
        tables: &mut IndexMap<String, Arc<Self>>,
        schema: &IndexMap<String, config::Field>,
        external_ids: Vec<String>,
        table: String,
    ) -> Result<(), Error> {
        let mut id_name = None;
        let mut hash_name = None;
        let fields = schema
            .iter()
            .map(|(name, def)| {
                let field = match &def {
                    config::Field::Id => {
                        id_name = Some(name.clone());
                        FieldType::Id
                    }
                    config::Field::Hash => {
                        hash_name = Some(name.clone());
                        FieldType::Hash
                    }
                    config::Field::String { required, index } => FieldType::String {
                        required: *required,
                        index: *index,
                    },
                    config::Field::Boolean { required, index } => FieldType::Boolean {
                        required: *required,
                        index: *index,
                    },
                    config::Field::Integer { required, index } => FieldType::Integer {
                        required: *required,
                        index: *index,
                    },
                    config::Field::Real { required, index } => FieldType::Real {
                        required: *required,
                        index: *index,
                    },
                    config::Field::Date { required, index } => FieldType::Date {
                        required: *required,
                        index: *index,
                    },
                    config::Field::Datetime { required, index } => FieldType::Datetime {
                        required: *required,
                        index: *index,
                    },
                    config::Field::Markdown {
                        required,
                        storage,
                        image,
                        config,
                    } => FieldType::Markdown {
                        required: *required,
                        storage: storage.clone(),
                        image: image.clone(),
                        config: config.clone(),
                    },
                    config::Field::Image {
                        required,
                        transform,
                        storage,
                        index,
                    } => FieldType::Image {
                        required: *required,
                        transform: transform.clone(),
                        storage: storage.clone(),
                        index: *index,
                    },
                    config::Field::File {
                        required,
                        storage,
                        index,
                    } => FieldType::File {
                        required: *required,
                        storage: storage.clone(),
                        index: *index,
                    },
                    config::Field::Records {
                        required,
                        parent_id_names,
                        schema,
                        table,
                        ..
                    } => {
                        Self::add_table(tables, schema, parent_id_names.clone(), table.clone())?;
                        FieldType::Records {
                            table: table.clone(),
                            required: *required,
                        }
                    }
                };
                Ok((name.clone(), field))
            })
            .collect::<Result<_, _>>()?;
        tables.insert(
            table,
            Arc::new(Self {
                id_name: id_name.ok_or(Error::IdUndefined)?,
                hash_name: hash_name,
                fields,
                compound_id_prefix_names: external_ids,
            }),
        );
        Ok(())
    }

    pub(crate) fn tables(config: &config::Collection) -> Result<TableSchemas, Error> {
        let mut tables = IndexMap::new();
        Self::add_table(
            &mut tables,
            &config.schema,
            Default::default(),
            config.table.clone(),
        )?;
        Ok(tables)
    }

    pub(crate) fn is_id_only_table(&self) -> bool {
        self.fields.len() == 1
    }
}
