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

#[derive(Debug, Clone)]
pub struct ParentTable {
    pub(crate) id_names: Vec<String>,
    pub(crate) name: String,
}

#[derive(Debug)]
pub struct Schema {
    pub(crate) parent: Option<ParentTable>,
    pub(crate) fields: IndexMap<String, FieldType>,
    pub(crate) inherit_ids: Vec<String>,
    pub(crate) id_name: String,
    pub(crate) hash_name: Option<String>,
}

pub(crate) type TableSchemas = IndexMap<String, Arc<Schema>>;

#[derive(Debug)]
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
        required: bool,
        storage: config::ImageStorage,
    },
    File {
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

impl Schema {
    fn add_table(
        tables: &mut IndexMap<String, Arc<Self>>,
        parent: Option<ParentTable>,
        schema: &IndexMap<String, config::Field>,
        inherit_ids: Vec<String>,
        table: String,
    ) -> Result<(), Error> {
        let id_name = schema
            .iter()
            .find_map(|(name, def)| {
                if matches!(def, config::Field::Id) {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .ok_or(Error::IdUndefined)?;
        let mut id_names = inherit_ids.clone();
        id_names.push(id_name.clone());
        let current_table_to_referenced = ParentTable {
            name: table.clone(),
            id_names,
        };
        let mut hash_name = None;
        let fields = schema
            .iter()
            .map(|(name, def)| {
                let field = match &def {
                    config::Field::Id => FieldType::Id,
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
                    config::Field::Image { required, storage } => FieldType::Image {
                        required: *required,
                        storage: storage.clone(),
                    },
                    config::Field::File { required, storage } => FieldType::File {
                        required: *required,
                        storage: storage.clone(),
                    },
                    config::Field::Records {
                        required,
                        inherit_ids,
                        schema,
                        table: child_table,
                        ..
                    } => {
                        Self::add_table(
                            tables,
                            Some(current_table_to_referenced.clone()),
                            schema,
                            inherit_ids.clone(),
                            child_table.clone(),
                        )?;
                        FieldType::Records {
                            table: child_table.clone(),
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
                parent,
                id_name,
                hash_name,
                fields,
                inherit_ids,
            }),
        );
        Ok(())
    }

    pub fn tables(config: &config::Collection) -> Result<TableSchemas, Error> {
        let mut tables = IndexMap::new();
        Self::add_table(
            &mut tables,
            None,
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
