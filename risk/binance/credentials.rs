use crate::binance::BinanceError;

#[derive(Debug, Clone)]
pub struct BinanceCredentials {
    pub api_key: String,
    pub api_secret: String,
}

impl BinanceCredentials {
    /// Construct credentials directly from config-file values.
    pub fn from_config(api_key: &str, api_secret: &str) -> Result<Self, BinanceError> {
        if api_key.is_empty() {
            return Err(BinanceError::MissingEnvVar {
                name: "api_key".to_owned(),
            });
        }
        if api_secret.is_empty() {
            return Err(BinanceError::MissingEnvVar {
                name: "api_secret".to_owned(),
            });
        }
        Ok(Self {
            api_key: api_key.to_owned(),
            api_secret: api_secret.to_owned(),
        })
    }
}
