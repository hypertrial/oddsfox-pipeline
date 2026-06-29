use std::io::{self, Write};

use chrono::Utc;

pub fn log_progress(message: impl AsRef<str>) {
    println!("{} {}", Utc::now().to_rfc3339(), message.as_ref());
    let _ = io::stdout().flush();
}
