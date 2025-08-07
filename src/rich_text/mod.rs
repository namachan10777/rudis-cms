use std::{
    borrow::Borrow, collections::HashMap, convert::Infallible, hash::Hash, ops::Deref, str::FromStr,
};

use pulldown_cmark::CowStr;
use serde::{Deserialize, Serialize};
use valuable::Valuable;

use crate::rich_text::codeblock::meta_parser::CodeblockMeta;

pub mod codeblock;
pub mod parser;

#[derive(Debug, Clone)]
pub enum AttrValue {
    Bool(bool),
    OwnedStr(String),
    StaticStr(&'static str),
    Integer(i64),
}

impl AttrValue {
    pub fn to_str(&self) -> Option<&str> {
        match self {
            Self::OwnedStr(s) => Some(&*s),
            Self::StaticStr(s) => Some(s),
            _ => None,
        }
    }
}

impl PartialEq for AttrValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::OwnedStr(_) | Self::StaticStr(_), Self::OwnedStr(_) | Self::StaticStr(_)) => {
                self.to_str() == other.to_str()
            }
            (Self::Integer(lhs), Self::Integer(rhs)) => lhs == rhs,
            (Self::Bool(lhs), Self::Bool(rhs)) => lhs == rhs,
            _ => false,
        }
    }
}

impl Eq for AttrValue {}

impl Valuable for AttrValue {
    fn as_value(&self) -> valuable::Value<'_> {
        match self {
            Self::Bool(b) => b.as_value(),
            Self::Integer(i) => i.as_value(),
            Self::OwnedStr(s) => s.as_value(),
            Self::StaticStr(s) => s.as_value(),
        }
    }

    fn visit(&self, visit: &mut dyn valuable::Visit) {
        match self {
            Self::Bool(b) => b.visit(visit),
            Self::Integer(i) => i.visit(visit),
            Self::OwnedStr(s) => s.visit(visit),
            Self::StaticStr(s) => s.visit(visit),
        }
    }
}

impl Serialize for AttrValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            AttrValue::Bool(value) => serializer.serialize_bool(*value),
            AttrValue::OwnedStr(value) => serializer.serialize_str(value),
            AttrValue::StaticStr(value) => serializer.serialize_str(value),
            AttrValue::Integer(value) => serializer.serialize_i64(*value),
        }
    }
}

impl<'de> Deserialize<'de> for AttrValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct AttrValueVisitor;

        impl<'de> serde::de::Visitor<'de> for AttrValueVisitor {
            type Value = AttrValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a boolean, string, static string, or integer")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(AttrValue::Bool(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(AttrValue::OwnedStr(value.to_string()))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(AttrValue::Integer(value))
            }
        }

        deserializer.deserialize_any(AttrValueVisitor)
    }
}

impl From<bool> for AttrValue {
    fn from(value: bool) -> Self {
        AttrValue::Bool(value)
    }
}

impl From<String> for AttrValue {
    fn from(value: String) -> Self {
        AttrValue::OwnedStr(value)
    }
}

impl From<&'static str> for AttrValue {
    fn from(value: &'static str) -> Self {
        AttrValue::StaticStr(value)
    }
}

impl From<i64> for AttrValue {
    fn from(value: i64) -> Self {
        AttrValue::Integer(value)
    }
}

impl From<CowStr<'static>> for AttrValue {
    fn from(value: CowStr<'static>) -> Self {
        match value {
            CowStr::Borrowed(b) => AttrValue::StaticStr(b),
            CowStr::Inlined(s) => AttrValue::OwnedStr(s.to_string()),
            CowStr::Boxed(s) => AttrValue::OwnedStr(s.into_string()),
        }
    }
}

impl FromStr for AttrValue {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "true" => Ok(Self::Bool(true)),
            "false" => Ok(Self::Bool(false)),
            s => Ok(s
                .parse::<i64>()
                .map(AttrValue::Integer)
                .unwrap_or_else(|_| AttrValue::OwnedStr(s.to_string()))),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Name {
    Static(&'static str),
    Owned(String),
}

impl Hash for Name {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::Static(s) => s.hash(state),
            Self::Owned(s) => s.hash(state),
        }
    }
}

impl AsRef<str> for Name {
    fn as_ref(&self) -> &str {
        match self {
            Self::Owned(s) => s,
            Self::Static(s) => s,
        }
    }
}

impl Deref for Name {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl PartialEq for Name {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for Name {}

impl Valuable for Name {
    fn as_value(&self) -> valuable::Value<'_> {
        match self {
            Self::Static(s) => s.as_value(),
            Self::Owned(s) => s.as_value(),
        }
    }

    fn visit(&self, visit: &mut dyn valuable::Visit) {
        match self {
            Self::Static(s) => s.visit(visit),
            Self::Owned(s) => s.visit(visit),
        }
    }
}

impl Serialize for Name {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Name::Static(value) => serializer.serialize_str(value),
            Name::Owned(value) => serializer.serialize_str(value),
        }
    }
}

impl<'de> Deserialize<'de> for Name {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct NameVisitor;

        impl<'de> serde::de::Visitor<'de> for NameVisitor {
            type Value = Name;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a static or owned string")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Name::Owned(value.to_string()))
            }
        }

        deserializer.deserialize_str(NameVisitor)
    }
}

impl From<&'static str> for Name {
    fn from(value: &'static str) -> Self {
        Name::Static(value)
    }
}

impl From<String> for Name {
    fn from(value: String) -> Self {
        Name::Owned(value)
    }
}

impl From<CowStr<'static>> for Name {
    fn from(value: CowStr<'static>) -> Self {
        match value {
            CowStr::Boxed(s) => Self::Owned(s.into_string()),
            CowStr::Borrowed(b) => Self::Static(b),
            CowStr::Inlined(s) => Self::Owned(s.to_string()),
        }
    }
}

impl Borrow<str> for Name {
    fn borrow(&self) -> &str {
        self.as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Valuable)]
#[serde(rename_all = "snake_case")]
pub enum AlertKind {
    Caution,
    Important,
    Note,
    Warning,
    Tip,
}

#[derive(Serialize, Deserialize, Debug, Clone, Valuable)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum LinkType {
    Autolink,
    Wikilink,
    Normal,
    Email,
    Broken,
}

#[derive(Debug, Clone, Valuable)]
pub enum Extracted {
    FootnoteReference {
        id: String,
    },
    Alert {
        kind: AlertKind,
    },
    Codeblock {
        meta: CodeblockMeta,
    },
    Heading {
        level: u8,
        attrs: HashMap<Name, AttrValue>,
    },
    Image {
        title: String,
        id: String,
        url: String,
    },
    Link {
        link_type: LinkType,
        dest_url: String,
        title: String,
        id: String,
    },
}

#[derive(Debug, Clone, Valuable)]
pub enum MdAst {
    Raw(String),
    Eager {
        tag: Name,
        attrs: HashMap<String, AttrValue>,
        children: Vec<MdAst>,
    },
    Text(String),
    Lazy {
        extracted: Extracted,
        children: Vec<MdAst>,
    },
}

#[derive(Debug, Clone)]
pub struct MdRoot {
    pub children: Vec<MdAst>,
    pub frontmatter: Option<serde_json::Value>,
    pub footnote_definitions: HashMap<String, MdAst>,
}

#[derive(Debug, Serialize, Deserialize, Valuable)]
#[serde(rename_all = "snake_case")]
pub enum Cooked {
    Eager {
        tag: Name,
        attrs: HashMap<String, AttrValue>,
        children: String,
    },
    Lazy {
        tag: Name,
        attrs: HashMap<String, AttrValue>,
        children: Vec<Cooked>,
    },
    Text {
        text: String,
    },
}
