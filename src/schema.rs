use indexmap::{IndexMap, indexmap};

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

#[derive(Debug, Clone, Copy, Hash)]
pub enum TableType {
    Main,
    Dependent,
    MarkdownImage,
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub(crate) parent: Option<ParentTable>,
    pub(crate) fields: IndexMap<String, FieldType>,
    pub(crate) inherit_ids: Vec<String>,
    pub(crate) id_name: String,
    pub(crate) hash_name: Option<String>,
}

#[derive(Clone)]
pub struct CollectionSchema {
    pub(crate) tables: IndexMap<String, TableSchema>,
}

#[derive(Debug, Clone)]
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

impl ParentTable {
    fn as_parent<S: AsRef<str>>(inherit_ids: &[S], id_name: &str, table_name: &str) -> Self {
        Self {
            name: table_name.into(),
            id_names: inherit_ids
                .iter()
                .map(|s| s.as_ref().to_owned())
                .chain(std::iter::once(id_name.to_owned()))
                .collect(),
        }
    }
}

impl TableSchema {
    fn add_table(
        tables: &mut IndexMap<String, Self>,
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
        let mut hash_name = None;
        let self_as_parent = ParentTable::as_parent(&inherit_ids, &id_name, &table);
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
                    } => {
                        tables.insert(
                            image.table.clone(),
                            TableSchema {
                                parent: Some(self_as_parent.clone()),
                                inherit_ids: image.inherit_ids.clone(),
                                id_name: "src_id".to_string(),
                                hash_name: None,
                                fields: indexmap! {
                                    "src_id".to_string() => FieldType::Id,
                                    "image".to_string() => FieldType::Image { required: true, storage: image.storage.clone() },
                                },
                            },
                        );
                        FieldType::Markdown {
                            required: *required,
                            storage: storage.clone(),
                            image: image.clone(),
                            config: config.clone(),
                        }
                    }
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
                            Some(self_as_parent.clone()),
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
            Self {
                parent,
                id_name,
                hash_name,
                fields,
                inherit_ids,
            },
        );
        Ok(())
    }

    pub fn compile(config: &config::Collection) -> Result<CollectionSchema, Error> {
        let mut tables = IndexMap::new();
        Self::add_table(
            &mut tables,
            None,
            &config.schema,
            Default::default(),
            config.table.clone(),
        )?;
        tables.reverse();
        Ok(CollectionSchema { tables })
    }

    pub(crate) fn is_id_only_table(&self) -> bool {
        self.fields.len() == 1
    }
}
