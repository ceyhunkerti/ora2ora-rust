use indicatif::{ProgressBar, ProgressStyle, HumanDuration};
use std::thread::{spawn};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::time::{Instant};

use oracle::{Connection, StmtParam};

use query::{split_sql, ext_sql};
use settings::Settings;
use log::{info, debug, trace};
use core::ExtMessage;
use console::{style, Emoji};

static FLAG: Emoji = Emoji("🏁  ", "");

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

fn find_count(settings: &Settings) -> u64 {
  debug!("Finding source count ...");

  let filter = if settings.source.filter.is_empty() {
    "".to_string()
  } else {
    format!("where {}", &settings.source.filter)
  };

  let hint = if settings.source.filter.is_empty() {
    "".to_string()
  } else {
    format!("/*+ {} */", &settings.source.hint)
  };

  let query = format!("select {} count(1) from {} {}", hint, settings.source.table, filter);

  let connection = Connection::connect(
      &(settings.source.username),
      &(settings.source.password),
      &(settings.source.url), &[]).unwrap();

  let result = connection.query_row(&query, &[]).unwrap().get::<_, u64>(0).unwrap();
  connection.close().unwrap();
  result
}

pub fn run() {
  let start_time = Instant::now();
  let settings = Settings::new().unwrap();

  let total = find_count(&settings);
  info!("Total records: {}", total);
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

  let pb = ProgressBar::new(total);
  pb.set_style(ProgressStyle::default_bar()
    .template("{spinner:.green} [{elapsed_precise}] [{bar:100.cyan/blue}] {bytes}/{total_bytes} ({eta})")
    .progress_chars("#>-"));

  let mut extracted: u64 = 0;
  for message in receiver {
    extracted += message.record_count;
    pb.set_position(extracted);
  }
  pb.finish();
  let m = format!("Completed in {}", HumanDuration(start_time.elapsed()));
  info!("{} {} {}", style("[2/4]").bold().dim(), FLAG, m);
}

fn ext(index: u8, sender: Sender<ExtMessage>, query: String) {
  let settings = Settings::new().unwrap();
  let mut message: ExtMessage = ExtMessage::default();
  let start_time = Instant::now();
  let connection = Connection::connect(
      &(settings.source.username),
      &(settings.source.password),
      &(settings.source.url), &[]).unwrap();

  let fetch_size = StmtParam::FetchArraySize(settings.source.fetch_size);
  let mut stmt = connection.prepare(&query, &[fetch_size]).unwrap();

  let rows = stmt.query(&[]).unwrap();
  let col_cnt = rows.column_info().len();

  let mut count = 0;
  for row in &rows {
    let r = row.unwrap();
    let record: String = (0 .. col_cnt)
      .map(|i| r.get::<_, String>(i).or::<String>(Ok("".to_string())).unwrap())
      .collect::<Vec<String>>()
      .join(",");

    // println!("{}", record);
    count += 1;
  }

  message.record_count = count;
  sender.send(message).unwrap();

  connection.close().unwrap();
  let duration = start_time.elapsed();
  debug!("Extraction of part {} completed in {} seconds", index, duration.as_secs());
  drop(sender);
}