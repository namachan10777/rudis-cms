use crate::{backend::Backend, config::FieldDef};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Type {
    Boolean,
    String,
    Integer,
    Real,
    Image,
    Array,
    Object,
    Null,
    Blob,
}

impl std::fmt::Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Boolean => f.write_str("boolean"),
            Self::Image => f.write_str("image"),
            Self::Integer => f.write_str("integer"),
            Self::String => f.write_str("string"),
            Self::Real => f.write_str("real"),
            Self::Array => f.write_str("array"),
            Self::Object => f.write_str("object"),
            Self::Null => f.write_str("null"),
            Self::Blob => f.write_str("blob"),
        }
    }
}

impl<'j> From<&'j serde_json::Value> for Type {
    fn from(value: &'j serde_json::Value) -> Self {
        match value {
            serde_json::Value::Array(_) => Self::Array,
            serde_json::Value::Bool(_) => Self::Boolean,
            serde_json::Value::Number(n) if n.is_i64() => Self::Integer,
            serde_json::Value::Number(_) => Self::Real,
            serde_json::Value::Object(_) => Self::Object,
            serde_json::Value::String(_) => Self::String,
            serde_json::Value::Null => Self::Null,
        }
    }
}

impl<'d, B: Backend> From<&'d FieldDef<B>> for Type {
    fn from(value: &'d FieldDef<B>) -> Self {
        match value {
            FieldDef::Blob { .. } => Self::Blob,
            FieldDef::Boolean { .. } => Self::Boolean,
            FieldDef::Datetime { .. } => Self::String,
            FieldDef::Hash { .. } => Self::Null,
            FieldDef::Integer { .. } => Self::Integer,
            FieldDef::String { .. } => Self::String,
            FieldDef::Id {} => Self::String,
            FieldDef::Json { .. } => Self::Object,
            FieldDef::Image { .. } => Self::Image,
            FieldDef::Markdown { .. } => Self::Object,
            FieldDef::Set { .. } => Self::Array,
        }
    }
}
