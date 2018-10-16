pub mod oracle;
use std::time::{Duration};

#[derive(Debug, Default)]
pub struct ExtMessage<'a, 'b> {
  pub query: &'a str,
  pub file_path: &'b str,
  pub record_count: u8,
  pub duration: Duration,
}

// use std::time::{Duration, Instant};

// fn main() {
//     let start = Instant::now();
//     expensive_function();
//     let duration = start.elapsed();

//     println!("Time elapsed in expensive_function() is: {:?}", duration);
// }