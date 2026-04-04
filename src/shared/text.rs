pub fn truncate_with_ascii_ellipsis(value: &str, max_bytes: usize) -> Option<String> {
    if max_bytes == 0 {
        return None;
    }
    if value.len() <= max_bytes {
        return Some(value.to_owned());
    }
    if max_bytes <= 3 {
        return Some(truncate_to_char_boundary(value, max_bytes).to_owned());
    }

    let truncated = truncate_to_char_boundary(value, max_bytes - 3);
    Some(format!("{truncated}..."))
}

pub fn truncate_to_char_boundary(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }

    let mut end = 0usize;
    for (index, _) in value.char_indices() {
        if index > max_bytes {
            break;
        }
        end = index;
    }

    if end == 0 && !value.is_empty() && max_bytes > 0 {
        let first_char_end = value
            .char_indices()
            .nth(1)
            .map(|(index, _)| index)
            .unwrap_or(value.len());
        if first_char_end <= max_bytes {
            &value[..first_char_end]
        } else {
            ""
        }
    } else {
        &value[..end]
    }
}

#[cfg(test)]
mod tests {
    use super::{truncate_to_char_boundary, truncate_with_ascii_ellipsis};

    #[test]
    fn truncate_to_char_boundary_returns_empty_when_first_scalar_exceeds_budget() {
        assert_eq!(truncate_to_char_boundary("🙂abc", 2), "");
    }

    #[test]
    fn truncate_with_ascii_ellipsis_respects_byte_budget_for_multibyte_prefix() {
        let truncated = truncate_with_ascii_ellipsis("🙂abc", 2).expect("truncated value");
        assert_eq!(truncated, "");
        assert!(truncated.len() <= 2);
    }
}
