use config::{ConfigError, Config, File};

#[derive(Debug, Deserialize)]
pub struct Source {
  pub table: String,
  pub url: String,
  pub username: String,
  pub password: String,
  pub filter: String,
  pub columns: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct Target {
  pub url: String,
  pub username: String,
  pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct Global {
  pub parallel: u8,
  pub unload_path: String,
}


#[derive(Debug, Deserialize)]
pub struct Settings {
  pub source: Source,
  pub target: Target,
  pub global: Global,
}

impl Settings {
  pub fn new() -> Result<Self, ConfigError> {
    let mut s = Config::new();
    s.merge(File::with_name("app"))?;
    s.try_into()
  }
}
