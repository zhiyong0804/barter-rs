use crate::binance::BinanceError;
use std::env;

#[derive(Debug, Clone)]
pub struct BinanceCredentials {
    pub api_key: String,
    pub api_secret: String,
}

impl BinanceCredentials {
    pub fn from_env_names(
        api_key_env: &str,
        api_secret_env: &str,
    ) -> Result<Self, BinanceError> {
        let api_key = env::var(api_key_env).map_err(|_| BinanceError::MissingEnvVar {
            name: api_key_env.to_owned(),
        })?;
        let api_secret = env::var(api_secret_env).map_err(|_| BinanceError::MissingEnvVar {
            name: api_secret_env.to_owned(),
        })?;

        Ok(Self {
            api_key,
            api_secret,
        })
    }
}