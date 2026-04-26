use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SymbolSpec {
    pub symbol: String,
    pub base: String,
    pub quote: String,
}
