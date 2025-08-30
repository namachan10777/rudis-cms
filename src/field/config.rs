use std::collections::HashSet;

use serde::Deserialize;

#[derive(Deserialize, Hash, PartialEq, Eq)]
pub enum RasterFormat {
    Png,
    Jpeg,
    Avif,
    Webp,
}

#[derive(Deserialize)]
pub enum ImageTransform {
    Simple {
        width: Option<u32>,
        format: Option<RasterFormat>,
    },
    Matrix {
        width: HashSet<u32>,
        format: HashSet<RasterFormat>,
        fallback: RasterFormat,
    },
}

#[derive(Deserialize)]
pub enum Item {
    String,
    Integer,
    Real,
    Boolean,
    Json,
    Image {
        transform: ImageTransform,
        #[serde(default)]
        config: serde_json::Value,
    },
    File {
        #[serde(default)]
        config: serde_json::Value,
    },
}

#[derive(Deserialize)]
pub enum FieldConfig {
    String {
        #[serde(default)]
        required: bool,
    },
    Integer {
        #[serde(default)]
        required: bool,
    },
    Real {
        #[serde(default)]
        required: bool,
    },
    Boolean {
        #[serde(default)]
        required: bool,
    },
    Json {
        #[serde(default)]
        required: bool,
        #[serde(default)]
        allow_null: bool,
    },
    Image {
        #[serde(default)]
        required: bool,
        transform: ImageTransform,
        #[serde(default)]
        config: serde_json::Value,
    },
    File {
        #[serde(default)]
        required: bool,
        #[serde(default)]
        config: serde_json::Value,
    },
    Id,
    Hash,
    Set {
        item: Item,
    },
}
