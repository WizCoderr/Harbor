use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const SECRET_HEADER_NAMES: &[&str] = &[
    "authorization",
    "cookie",
    "proxy-authorization",
    "x-api-key",
];

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecretMaterial {
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub cookies: Option<String>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

impl SecretMaterial {
    pub fn is_empty(&self) -> bool {
        self.username.as_deref().unwrap_or("").is_empty()
            && self.password.as_deref().unwrap_or("").is_empty()
            && self.cookies.as_deref().unwrap_or("").is_empty()
            && self.headers.is_empty()
    }
}

pub fn split_headers(headers: HashMap<String, String>) -> (HashMap<String, String>, HashMap<String, String>) {
    let mut public = HashMap::new();
    let mut secret = HashMap::new();
    for (key, value) in headers {
        if value.chars().any(|c| c == '\r' || c == '\n') || key.chars().any(|c| c == '\r' || c == '\n') {
            continue;
        }
        if SECRET_HEADER_NAMES.iter().any(|name| key.eq_ignore_ascii_case(name)) {
            secret.insert(key, value);
        } else {
            public.insert(key, value);
        }
    }
    (public, secret)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authorization_is_not_a_public_header() {
        let mut headers = HashMap::new();
        headers.insert("Authorization".into(), "Bearer secret-token".into());
        headers.insert("Referer".into(), "https://example.test".into());
        let (public, secret) = split_headers(headers);
        assert!(public.contains_key("Referer"));
        assert!(!public.keys().any(|k| k.eq_ignore_ascii_case("authorization")));
        assert!(secret.values().any(|v| v == "Bearer secret-token"));
    }
}
