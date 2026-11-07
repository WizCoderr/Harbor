use crate::error::EngineError;
use crate::http::map_reqwest;
use crate::EngineHandle;
use harbor_core::{file_extension, filename_from_url, GrabbedLink};
use regex::Regex;
use std::collections::{HashSet, VecDeque};
use std::sync::OnceLock;
use url::Url;

pub async fn preview(
    engine: &EngineHandle,
    page_url: &str,
    extensions: &[String],
    domain: Option<&str>,
    depth: u32,
) -> Result<Vec<GrabbedLink>, EngineError> {
    let start = Url::parse(page_url).map_err(|_| EngineError::Fatal("Invalid URL".into()))?;
    if start.scheme() != "http" && start.scheme() != "https" {
        return Err(EngineError::Fatal("The grabber only reads http and https pages.".into()));
    }
    let depth = depth.clamp(1, 2);
    let wanted: HashSet<String> = if extensions.is_empty() {
        default_extensions().into_iter().map(|ext| ext.to_string()).collect()
    } else {
        extensions.iter().map(|ext| ext.trim().trim_start_matches('.').to_ascii_lowercase()).filter(|ext| !ext.is_empty()).collect()
    };
    let domain = domain.map(|value| value.trim().trim_start_matches("*.").to_ascii_lowercase()).filter(|value| !value.is_empty());
    let client = engine.client.read().expect("client").clone();
    let mut queue = VecDeque::from([(start.clone(), 1u32)]);
    let mut seen_pages = HashSet::new();
    let mut seen_files = HashSet::new();
    let mut found = Vec::new();
    while let Some((page, level)) = queue.pop_front() {
        if !seen_pages.insert(page.to_string()) || seen_pages.len() > 20 {
            continue;
        }
        let response = client.get(page.clone()).header(reqwest::header::ACCEPT, "text/html,*/*").send().await.map_err(map_reqwest)?;
        if !response.status().is_success() {
            continue;
        }
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_ascii_lowercase();
        if !content_type.is_empty() && !content_type.contains("html") && !content_type.contains("text/") {
            continue;
        }
        let body = response.text().await.map_err(map_reqwest)?;
        for link in extract_links(&page, &body) {
            let ext = file_extension(link.path());
            let host_ok = domain.as_deref().map(|domain| host_matches(link.host_str().unwrap_or(""), domain)).unwrap_or(true);
            if !ext.is_empty() && wanted.contains(&ext) && host_ok && seen_files.insert(link.to_string()) {
                found.push(GrabbedLink {
                    filename: filename_from_url(link.as_str()).unwrap_or_else(|| "download".into()),
                    extension: ext,
                    url: link.to_string(),
                });
                continue;
            }
            if level < depth && link.host() == page.host() && (ext.is_empty() || ext == "html" || ext == "htm") {
                queue.push_back((link, level + 1));
            }
        }
    }
    found.sort_by(|a, b| a.url.cmp(&b.url));
    Ok(found)
}

fn extract_links(base: &Url, html: &str) -> Vec<Url> {
    link_pattern()
        .captures_iter(html)
        .filter_map(|capture| capture.get(1).map(|value| value.as_str()))
        .filter_map(|raw| base.join(raw.trim()).ok())
        .filter(|url| matches!(url.scheme(), "http" | "https"))
        .collect()
}

fn link_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| Regex::new(r#"(?i)(?:href|src)\s*=\s*["']([^"'#]+)["']"#).expect("link pattern"))
}

fn host_matches(host: &str, domain: &str) -> bool {
    let host = host.to_ascii_lowercase();
    host == domain || host.ends_with(&format!(".{domain}"))
}

fn default_extensions() -> &'static [&'static str] {
    &[
        "zip", "rar", "7z", "tar", "gz", "bz2", "xz", "iso", "exe", "msi", "dmg", "pkg", "deb", "rpm", "apk", "pdf",
        "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "csv", "mp4", "mkv", "webm", "mov", "avi", "mp3", "flac",
        "wav", "aac", "m4a", "ogg", "bin",
    ]
}
