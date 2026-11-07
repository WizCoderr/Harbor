use percent_encoding::percent_decode_str;

pub fn filename_from_content_disposition(value: &str) -> Option<String> {
    if let Some(name) = extended_filename(value) {
        let clean = sanitize_filename(&name);
        if clean != "download" || name == "download" {
            return Some(clean);
        }
    }
    for part in value.split(';').skip(1) {
        let part = part.trim();
        let Some((key, raw)) = part.split_once('=') else {
            continue;
        };
        if !key.trim().eq_ignore_ascii_case("filename") {
            continue;
        }
        let raw = raw.trim().trim_matches('"');
        if raw.is_empty() || raw.contains("''") && key.contains('*') {
            continue;
        }
        return Some(sanitize_filename(&percent_decode_str(raw).decode_utf8_lossy()));
    }
    None
}

fn extended_filename(value: &str) -> Option<String> {
    for part in value.split(';').skip(1) {
        let part = part.trim();
        let Some((key, raw)) = part.split_once('=') else {
            continue;
        };
        if !key.trim().eq_ignore_ascii_case("filename*") {
            continue;
        }
        let raw = raw.trim().trim_matches('"');
        let encoded = raw
            .split_once("''")
            .map(|(_, rest)| rest)
            .unwrap_or(raw);
        let decoded = percent_decode_str(encoded).decode_utf8_lossy();
        if !decoded.trim().is_empty() {
            return Some(decoded.into_owned());
        }
    }
    None
}

pub fn filename_from_url(raw: &str) -> Option<String> {
    let url = url::Url::parse(raw).ok()?;
    let name = url.path_segments()?.next_back()?.trim();
    if name.is_empty() {
        return None;
    }
    let decoded = percent_decode_str(name).decode_utf8_lossy();
    Some(sanitize_filename(&decoded))
}

pub fn sanitize_filename(name: &str) -> String {
    let trimmed = name.trim().trim_matches(|c| c == '"' || c == '\'');
    let base = std::path::Path::new(trimmed)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("download");
    let cleaned: String = base
        .chars()
        .map(|c| {
            if c.is_control() || matches!(c, '/' | '\\' | ':' | '*' | '?' | '<' | '>' | '|') {
                '_'
            } else {
                c
            }
        })
        .collect();
    let cleaned = cleaned.trim().trim_start_matches('.');
    if cleaned.is_empty() || cleaned == "." || cleaned == ".." {
        "download".into()
    } else {
        cleaned.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_quoted_and_extended_names() {
        assert_eq!(
            filename_from_content_disposition("attachment; filename=\"report.pdf\"").as_deref(),
            Some("report.pdf")
        );
        assert_eq!(
            filename_from_content_disposition("attachment; filename*=UTF-8''quarter%20plan.pdf")
                .as_deref(),
            Some("quarter plan.pdf")
        );
    }

    #[test]
    fn strips_url_path_and_query() {
        assert_eq!(
            filename_from_url("https://cdn.example/files/video.mp4?token=1").as_deref(),
            Some("video.mp4")
        );
    }

    #[test]
    fn rejects_path_components() {
        assert_eq!(sanitize_filename("../../etc/passwd"), "passwd");
        assert_eq!(sanitize_filename("   "), "download");
    }
}
