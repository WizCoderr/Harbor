fn main() {
    if let Err(err) = harbor_agent::run() {
        eprintln!("Harbor agent: {err}");
        std::process::exit(1);
    }
}
