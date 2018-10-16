use std::thread::{spawn};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::time::{Instant};

use oracle::{Connection, StmtParam};

use query::{split_sql, ext_sql};
use settings::Settings;
use log::{info, debug, trace};
use core::ExtMessage;

fn find_ranges(settings: &Settings) -> Vec<(String, String)> {
  info!("Finding ranges...");

  let table: Vec<&str> = settings.source.table.split(".").collect();
  let splitq = split_sql(table[0], table[1], settings.global.parallel);
  trace!("{}", &splitq);
  let connection = Connection::connect(
      &(settings.source.username),
      &(settings.source.password),
      &(settings.source.url), &[]).unwrap();

  let rows = connection.query_as::<(String, String)>(&splitq, &[]).unwrap();
  let mut result: Vec<(String, String)> = Vec::new();

  for r in &rows {
    let (from, to) = r.unwrap();
    result.push((from, to))
  }
  connection.close().unwrap();
  info!("{} ranges found", result.len());
  return result;
}

pub fn run() {
  let settings = Settings::new().unwrap();
  let ranges = find_ranges(&settings);

  let columns = if settings.source.columns.is_empty() {
    None
  } else {
    Some(&settings.source.columns)
  };

  let filter: Option<&str> = if settings.source.filter.is_empty() {
    None
  } else {
    Some(&settings.source.filter)
  };

  let (sender , receiver): (Sender<ExtMessage>, Receiver<ExtMessage>) = channel();
  let mut index: u8 = 0;
  for range in ranges {
    index += 1;
    let query = ext_sql(&settings.source.table, columns, filter, range);
    debug!("{}", &query);
    let s = Sender::clone(&sender);
    spawn(move || {
      ext(index, s, query)
    });
  }
  // drop the original sender
  drop(sender);
  for message in receiver {
    println!("Got: {}", message.record_count);
  }
}

fn ext(index: u8, sender: Sender<ExtMessage>, query: String) {
  let settings = Settings::new().unwrap();
  let message = ExtMessage::default();
  let start_time = Instant::now();
  let connection = Connection::connect(
      &(settings.source.username),
      &(settings.source.password),
      &(settings.source.url), &[]).unwrap();

  let fetch_size = StmtParam::FetchArraySize(settings.source.fetch_size);
  let mut stmt = connection.prepare(&query, &[fetch_size]).unwrap();

  let rows = stmt.query(&[]).unwrap();
  let column_info = rows.column_info();
  let col_cnt = rows.column_info().len();
  println!("{} column count {}", index, column_info.len());

  for row in &rows {
    let r = row.unwrap();

    let record: String = (0 .. col_cnt)
      .map(|i| r.get::<_, String>(i)
      .unwrap())
      .collect::<Vec<String>>()
      .join(",");

    println!("{}", record);
  }

  sender.send(message).unwrap();

  connection.close().unwrap();
  let duration = start_time.elapsed();
  debug!("Extraction of part {} completed in {} seconds", index, duration.as_secs());
  drop(sender);
}