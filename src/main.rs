// src/main.rs
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  tracing_subscriber::fmt()
    .with_env_filter(
      EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    )
    .init();

  let args: Vec<String> = std::env::args().collect();
  let cmd = args.get(1).map(|s| s.as_str()).unwrap_or("serve");
  match cmd {
    "serve" => {
      let path = args.get(2).cloned().unwrap_or_else(|| "config.toml".into());
      let cfg = tokenbalancer::config::load(&path)?;
      let store = std::sync::Arc::new(tokenbalancer::store::Store::open(&cfg.server.db_path)?);
      let runtime = tokenbalancer::state::Runtime::load(&cfg, store).await?;
      let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .build()?;
      let state = tokenbalancer::proxy::AppState {
        runtime,
        client,
        queue_timeout: Duration::from_secs(cfg.server.queue_timeout_secs),
      };
      let app = tokenbalancer::proxy::router(state)
        .layer(tower_http::cors::CorsLayer::permissive());
      let listener = tokio::net::TcpListener::bind(&cfg.server.listen).await?;
      let port = cfg.server.listen.rsplit(':').next().unwrap_or("8787");
      println!("TokenBalancer 已启动  http://{port}");
      println!("  代理 (OpenAI):   http://<host>:{port}/v1");
      println!("  代理 (Anthropic): http://<host>:{port}/apps/anthropic");
      println!("  管理页面:       http://<host>:{port}/");
      axum::serve(listener, app).await?;
      Ok(())
    }
    "key" => {
      // generate a proxy key to paste into config [[users]] or the web UI
      println!("{}", tokenbalancer::web::gen_user_key());
      Ok(())
    }
    _ => {
      eprintln!("用法: tokenbalancer serve [config.toml] | tokenbalancer key new");
      Ok(())
    }
  }
}
