extern crate oracle;
extern crate config;
extern crate serde;
extern crate indicatif;
extern crate console;

// #[macro_use]
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate serde_derive;

mod query;
mod core;
mod settings;


fn main() {
  env_logger::init();
  core::oracle::run();
}
