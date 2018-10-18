pub mod oracle;
// use std::time::{Duration};

#[derive(Debug, Default)]
pub struct ExtMessage {
  pub total_count: u64,
  pub write_count: u64,
}