pub(crate) const RELEASE_VERSION: &str = match option_env!("SEINE_GIT_TAG") {
    Some(value) => value,
    None => match option_env!("BNMINER_GIT_TAG") {
        Some(value) => value,
        None => env!("CARGO_PKG_VERSION"),
    },
};

pub(crate) fn release_version() -> &'static str {
    RELEASE_VERSION
}

pub(crate) fn ui_display_version() -> String {
    format_ui_version(release_version())
}

fn format_ui_version(version: &str) -> String {
    if version.starts_with('v') || version.starts_with('V') {
        return version.to_string();
    }

    if version.chars().next().is_some_and(|ch| ch.is_ascii_digit()) {
        format!("v{version}")
    } else {
        version.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::format_ui_version;

    #[test]
    fn ui_version_prefixes_numeric_versions() {
        assert_eq!(format_ui_version("0.1.0"), "v0.1.0");
    }

    #[test]
    fn ui_version_keeps_existing_v_prefix() {
        assert_eq!(format_ui_version("v0.1.0-rc1"), "v0.1.0-rc1");
    }

    #[test]
    fn ui_version_keeps_non_numeric_tags() {
        assert_eq!(format_ui_version("release-candidate"), "release-candidate");
    }
}
