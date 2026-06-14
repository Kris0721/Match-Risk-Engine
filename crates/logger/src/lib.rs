mod logger;

pub use logger::*;

pub fn info(msg: &str) {
    eprintln!("[INFO]  {}", msg);
}

pub fn warn(msg: &str) {
    eprintln!("[WARN]  {}", msg);
}

pub fn error(msg: &str) {
    eprintln!("[ERROR] {}", msg);
}
