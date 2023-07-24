use config::{Config, ConfigError, File};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Settings {
    pub mongo_liveroom: String,
    pub mongo_worker: String,
    pub area_weight: HashMap<String, i64>,
    pub rooms_per_worker: i32,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let s = Config::builder()
            .add_source(File::with_name("configs"))
            .build()?;
        s.try_deserialize()
    }
}
