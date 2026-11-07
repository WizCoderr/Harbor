use crate::error::{http_failure, EngineError};
use crate::EngineHandle;
use harbor_core::{filename_from_content_disposition, filename_from_url, SecretMaterial};
use harbor_store::Store;
use reqwest::header::{ACCEPT, ACCEPT_ENCODING, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_RANGE, LAST_MODIFIED, RANGE, USER_AGENT};
use reqwest::{Client, RequestBuilder, StatusCode, Url};

pub struct RemoteInfo {
    pub final_url: String,
    pub filename: String,
    pub total_bytes: Option<u64>,
    pub mime: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub accept_ranges: bool,
    pub http_status: u16,
    pub resume_capable: bool,
}

pub fn build_client(config: &crate::EngineConfig, proxy_url: &str) -> Result<Client, EngineError> {
    let mut builder = Client::builder()
        .redirect(reqwest::redirect::Policy::limited(10))
        .tcp_keepalive(std::time::Duration::from_secs(30))
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .pool_max_idle_per_host(32)
        .connect_timeout(std::time::Duration::from_secs(20));
    if !proxy_url.trim().is_empty() {
        let proxy = reqwest::Proxy::all(proxy_url.trim()).map_err(|err| {
            EngineError::Fatal(format!("The proxy URL is not valid: {err}"))
        })?;
        builder = builder.proxy(proxy);
    }
    if config.danger_accept_invalid_certs {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if let Some(pem) = &config.extra_root_pem {
        let cert = reqwest::Certificate::from_pem(pem.as_bytes())
            .map_err(|err| EngineError::Fatal(format!("Could not load the test certificate: {err}")))?;
        builder = builder.add_root_certificate(cert);
    }
    builder.build().map_err(|err| EngineError::Fatal(format!("Could not build the HTTP client: {err}")))
}

pub fn load_secret(store: &Store, secrets: &dyn crate::Secrets, download: &harbor_core::Download) -> SecretMaterial {
    let mut material = SecretMaterial::default();
    if download.has_credentials {
        if let Ok(Some(raw)) = secrets.get(&format!("download:{}", download.id)) {
            if let Ok(parsed) = serde_json::from_str::<SecretMaterial>(&raw) {
                material = parsed;
            }
        }
    }
    if material.username.as_deref().unwrap_or("").is_empty() && material.password.as_deref().unwrap_or("").is_empty() {
        if let Ok(url) = Url::parse(&download.url) {
            if let Some(host) = url.host_str() {
                if let Ok(Some(refer)) = store.site_credential_ref(host) {
                    if let Ok(Some(raw)) = secrets.get(&refer) {
                        if let Ok(site) = serde_json::from_str::<SecretMaterial>(&raw) {
                            if material.username.as_deref().unwrap_or("").is_empty() {
                                material.username = site.username;
                            }
                            if material.password.as_deref().unwrap_or("").is_empty() {
                                material.password = site.password;
                            }
                            if material.cookies.as_deref().unwrap_or("").is_empty() {
                                material.cookies = site.cookies;
                            }
                            for (key, value) in site.headers {
                                material.headers.entry(key).or_insert(value);
                            }
                        }
                    }
                }
            }
        }
    }
    material
}

pub fn authorized_request(
    client: &Client,
    url: &str,
    download: &harbor_core::Download,
    secret: &SecretMaterial,
    user_agent: &str,
) -> Result<RequestBuilder, EngineError> {
    let mut request = client.get(url);
    request = request.header(ACCEPT_ENCODING, "identity").header(ACCEPT, "*/*").header(USER_AGENT, user_agent);
    if let (Some(username), password) = (
        secret.username.as_deref().filter(|v| !v.is_empty()),
        secret.password.clone(),
    ) {
        request = request.basic_auth(username, password);
    }
    for (key, value) in &download.headers {
        request = request.header(key, value);
    }
    for (key, value) in &secret.headers {
        request = request.header(key, value);
    }
    if let Some(cookies) = secret.cookies.as_deref().filter(|v| !v.is_empty()) {
        request = request.header(reqwest::header::COOKIE, cookies);
    }
    Ok(request)
}

impl EngineHandle {
    pub(crate) async fn probe_download(&self, download: &harbor_core::Download) -> Result<RemoteInfo, EngineError> {
        let client = self.client.read().expect("client").clone();
        let secret = load_secret(&self.store, self.secrets.as_ref(), download);
        self.probe_url(&client, &download.url, download, &secret).await
    }

    pub(crate) async fn probe_url(
        &self,
        client: &Client,
        url: &str,
        download: &harbor_core::Download,
        secret: &SecretMaterial,
    ) -> Result<RemoteInfo, EngineError> {
        let request = authorized_request(client, url, download, secret, &self.config.user_agent)?
            .header(RANGE, "bytes=0-0");
        let response = request.send().await.map_err(map_reqwest)?;
        let status = response.status();
        let final_url = response.url().to_string();
        let headers = response.headers().clone();
        drop(response);
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return Err(http_failure(status.as_u16()));
        }
        if status == StatusCode::NOT_FOUND || status == StatusCode::GONE {
            return Err(http_failure(status.as_u16()));
        }
        if !status.is_success() && status != StatusCode::PARTIAL_CONTENT {
            return Err(http_failure(status.as_u16()));
        }
        let total_bytes = total_from_headers(&headers, status);
        let ranged = status == StatusCode::PARTIAL_CONTENT && total_bytes.is_some();
        let filename = headers
            .get(CONTENT_DISPOSITION)
            .and_then(|v| v.to_str().ok())
            .and_then(filename_from_content_disposition)
            .or_else(|| filename_from_url(&final_url))
            .unwrap_or_else(|| "download".into());
        let mime = headers
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|value| value.split(';').next().unwrap_or(value).trim().to_string())
            .filter(|value| !value.is_empty());
        Ok(RemoteInfo {
            final_url,
            filename,
            total_bytes,
            mime,
            etag: header_string(&headers, ETAG),
            last_modified: header_string(&headers, LAST_MODIFIED),
            accept_ranges: ranged,
            http_status: status.as_u16(),
            resume_capable: ranged,
        })
    }
}

pub fn total_from_headers(headers: &reqwest::header::HeaderMap, status: StatusCode) -> Option<u64> {
    if status == StatusCode::PARTIAL_CONTENT {
        if let Some(value) = headers.get(CONTENT_RANGE).and_then(|v| v.to_str().ok()) {
            if let Some(total) = value.rsplit('/').next() {
                if total != "*" {
                    if let Ok(total) = total.parse::<u64>() {
                        return Some(total);
                    }
                }
            }
        }
    }
    headers
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
}

pub fn header_string(headers: &reqwest::header::HeaderMap, name: reqwest::header::HeaderName) -> Option<String> {
    headers.get(name).and_then(|v| v.to_str().ok()).map(|v| v.to_string()).filter(|v| !v.is_empty())
}

pub fn content_range_start(value: &str) -> Option<u64> {
    let rest = value.trim().strip_prefix("bytes ")?;
    rest.split(['-', '/']).next()?.parse().ok()
}

pub fn map_reqwest(err: reqwest::Error) -> EngineError {
    let message = err.to_string();
    if message.to_ascii_lowercase().contains("proxy") {
        return EngineError::Fatal(format!("Could not use the proxy: {message}"));
    }
    EngineError::Retryable(message)
}

pub fn apply_validator(builder: RequestBuilder, etag: Option<&str>, last_modified: Option<&str>) -> RequestBuilder {
    if let Some(etag) = etag.filter(|v| !v.is_empty()) {
        builder.header(IF_RANGE, etag)
    } else if let Some(modified) = last_modified.filter(|v| !v.is_empty()) {
        builder.header(IF_RANGE, modified)
    } else {
        builder
    }
}

