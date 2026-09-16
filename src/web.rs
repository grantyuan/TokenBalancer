// src/web.rs (stub, replaced in Task 10)
use axum::response::IntoResponse;
pub async fn static_index() -> impl IntoResponse { "TokenBalancer" }
pub async fn static_app_js() -> impl IntoResponse { "" }
pub async fn static_styles() -> impl IntoResponse { "" }
pub async fn whoami() -> &'static str { "{}" }
pub async fn accounts_health() -> &'static str { "{}" }
pub async fn me_usage() -> &'static str { "{}" }
pub async fn admin_accounts() -> &'static str { "{}" }
pub async fn admin_users() -> &'static str { "{}" }
pub async fn admin_create_user() -> &'static str { "{}" }
pub async fn admin_revoke_user() -> &'static str { "{}" }
pub async fn admin_patch_account() -> &'static str { "{}" }
pub async fn admin_reconcile() -> &'static str { "{}" }
pub async fn admin_clear_exhausted() -> &'static str { "{}" }
pub async fn admin_analytics() -> &'static str { "{}" }
