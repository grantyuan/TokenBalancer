// src/main.rs
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env().add_directive("tokenbalancer=info".parse().expect("static directive parses"))).init();
  let args: Vec<String> = std::env::args().collect();
  let cmd = args.get(1).map(|s| s.as_str()).unwrap_or("serve");
  match cmd {
    "serve" => {
      let path = args.get(2).cloned().unwrap_or_else(|| "config.toml".into());
      let cfg = tokenbalancer::config::load(&path)?;
      println!("tokenbalancer: {} account(s), config OK (listen {})", cfg.accounts.len(), cfg.server.listen);
      // full server starts in Task 9; until then just report
      Ok(())
    }
    _ => { eprintln!("usage: tokenbalancer [serve [config.toml]]"); Ok(()) }
  }
}
