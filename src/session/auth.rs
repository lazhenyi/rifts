//! Authentication — pluggable via the `AuthProvider` trait.
//!
//! Default `TokenAuth` accepts opaque bearer tokens. The trait is
//! async so implementations may call out to an external IdP.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::error::Result;
use crate::protocol::hello::AuthMode;
use crate::session::session::ClientId;

/// Authenticated principal returned to the server.
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub client_id: ClientId,
    pub claims: serde_json::Value,
    pub mode: AuthMode,
    /// Free-form region/device/risk hints (spec §17.2).
    pub hints: AuthHints,
}

/// Optional authorization context hints.
#[derive(Debug, Clone, Default)]
pub struct AuthHints {
    pub region: Option<String>,
    pub device: Option<String>,
    pub risk: Option<u32>,
}

/// Trait implemented by authentication backends.
#[async_trait]
pub trait AuthProvider: Send + Sync {
    /// Authenticate a bearer-style token.
    async fn authenticate(&self, mode: AuthMode, token: Option<&str>) -> Result<AuthContext>;

    /// Revoke a session — used on logout, token expiry, etc.
    async fn revoke(&self, client_id: &ClientId) -> Result<()>;
}

/// A trivial in-memory token store. Suitable for tests and
/// single-process deployments; production code should plug in a real
/// IdP.
pub struct TokenAuth {
    tokens: RwLock<HashMap<String, AuthContext>>,
}

impl TokenAuth {
    pub fn new() -> Self {
        Self {
            tokens: RwLock::new(HashMap::new()),
        }
    }

    /// Register a token → context mapping.
    pub fn register(&self, token: impl Into<String>, ctx: AuthContext) {
        self.tokens.write().insert(token.into(), ctx);
    }
}

impl Default for TokenAuth {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AuthProvider for TokenAuth {
    async fn authenticate(&self, mode: AuthMode, token: Option<&str>) -> Result<AuthContext> {
        if mode == AuthMode::Anonymous {
            return Ok(AuthContext {
                client_id: ClientId::new(format!("anon-{}", rand_suffix())),
                claims: serde_json::json!({}),
                mode,
                hints: AuthHints::default(),
            });
        }
        let token = token
            .ok_or_else(|| crate::error::RiftError::Auth(crate::error::AuthReject::Required))?;
        self.tokens.read().get(token).cloned().ok_or_else(|| {
            crate::error::RiftError::Auth(crate::error::AuthReject::Invalid("unknown token".into()))
        })
    }

    async fn revoke(&self, _client_id: &ClientId) -> Result<()> {
        Ok(())
    }
}

fn rand_suffix() -> String {
    ulid::Ulid::new().to_string()
}

/// A no-op auth provider for tests.
pub struct AllowAllAuth;

#[async_trait]
impl AuthProvider for AllowAllAuth {
    async fn authenticate(&self, _mode: AuthMode, _token: Option<&str>) -> Result<AuthContext> {
        Ok(AuthContext {
            client_id: ClientId::new("anonymous"),
            claims: serde_json::json!({}),
            mode: AuthMode::Anonymous,
            hints: AuthHints::default(),
        })
    }

    async fn revoke(&self, _client_id: &ClientId) -> Result<()> {
        Ok(())
    }
}

/// Wrap any `AuthProvider` in an `Arc`.
pub fn shared<A: AuthProvider + 'static>(a: A) -> Arc<dyn AuthProvider> {
    Arc::new(a)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn token_auth_round_trip() {
        let auth = TokenAuth::new();
        auth.register(
            "tok-1",
            AuthContext {
                client_id: ClientId::new("user-1"),
                claims: serde_json::json!({"sub": "user-1"}),
                mode: AuthMode::Bearer,
                hints: AuthHints::default(),
            },
        );
        let ctx = auth
            .authenticate(AuthMode::Bearer, Some("tok-1"))
            .await
            .unwrap();
        assert_eq!(ctx.client_id.as_str(), "user-1");
        assert!(
            auth.authenticate(AuthMode::Bearer, Some("nope"))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn anonymous_auth_works_without_token() {
        let auth = TokenAuth::new();
        let ctx = auth.authenticate(AuthMode::Anonymous, None).await.unwrap();
        assert_eq!(ctx.mode, AuthMode::Anonymous);
    }
}
