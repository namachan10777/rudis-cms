use serde::Deserialize;
use valuable::Valuable;

pub mod asset;
pub mod d1;
pub mod kv;
pub mod r2;

#[derive(Deserialize, Debug, Clone, PartialEq, Eq, Hash, Valuable)]
pub struct ResponseInfoPointer {
    pub pointer: Option<String>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq, Hash, Valuable)]
pub struct ResponseInfo {
    pub code: u16,
    pub message: String,
    pub documentation_url: Option<String>,
    pub source: Option<ResponseInfoPointer>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Response<R> {
    #[serde(default)]
    pub errors: Vec<ResponseInfo>,
    #[serde(default)]
    pub messages: Vec<ResponseInfo>,
    #[serde(default)]
    pub success: bool,
    #[serde(default)]
    pub result: R,
}
