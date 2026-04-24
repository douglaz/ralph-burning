use crate::contexts::milestone_record::model::MilestoneId;

pub fn milestone_bead_refs_match(milestone_id: &MilestoneId, left: &str, right: &str) -> bool {
    let left = left.trim();
    let right = right.trim();
    if left == right {
        return true;
    }

    let qualified_prefix = format!("{}.", milestone_id.as_str());
    left.strip_prefix(&qualified_prefix)
        .filter(|short_ref| is_local_milestone_alias(short_ref))
        .is_some_and(|short_ref| short_ref == right)
        || right
            .strip_prefix(&qualified_prefix)
            .filter(|short_ref| is_local_milestone_alias(short_ref))
            .is_some_and(|short_ref| short_ref == left)
        || canonicalize_milestone_bead_ref(milestone_id, left)
            == canonicalize_milestone_bead_ref(milestone_id, right)
}

fn is_local_milestone_alias(bead_id: &str) -> bool {
    !bead_id.contains('.') || looks_like_short_dotted_bead_ref(bead_id)
}

fn looks_like_short_dotted_bead_ref(bead_id: &str) -> bool {
    bead_id.contains('.')
        && bead_id
            .split('.')
            .all(|segment| !segment.is_empty() && segment.chars().all(|ch| ch.is_ascii_digit()))
}

fn milestone_id_is_numeric(milestone_id: &MilestoneId) -> bool {
    milestone_id.as_str().chars().all(|ch| ch.is_ascii_digit())
}

pub fn canonicalize_milestone_bead_ref(milestone_id: &MilestoneId, bead_id: &str) -> String {
    let trimmed = bead_id.trim();
    let qualified_prefix = format!("{}.", milestone_id.as_str());
    if trimmed.starts_with(&qualified_prefix) {
        trimmed.to_owned()
    } else if !trimmed.contains('.')
        || (looks_like_short_dotted_bead_ref(trimmed) && !milestone_id_is_numeric(milestone_id))
    {
        format!("{qualified_prefix}{trimmed}")
    } else {
        trimmed.to_owned()
    }
}

pub fn br_show_output_indicates_missing(stderr: &str, stdout: &str) -> bool {
    fn message_describes_missing_bead(message: &str) -> bool {
        let message = message.to_ascii_lowercase();
        message.contains("bead not found") || message.contains("issue not found")
    }

    message_describes_missing_bead(stderr) || message_describes_missing_bead(stdout)
}

#[cfg(test)]
mod tests {
    use super::{
        br_show_output_indicates_missing, canonicalize_milestone_bead_ref,
        milestone_bead_refs_match,
    };
    use crate::contexts::milestone_record::model::MilestoneId;

    #[test]
    fn canonicalizes_short_refs_and_numeric_aliases_without_rewriting_foreign_qualified_ids() {
        let milestone_id = MilestoneId::new("9ni").expect("milestone id");

        assert_eq!(
            canonicalize_milestone_bead_ref(&milestone_id, "bead-2"),
            "9ni.bead-2"
        );
        assert_eq!(
            canonicalize_milestone_bead_ref(&milestone_id, "8.5.3"),
            "9ni.8.5.3"
        );
        assert_eq!(
            canonicalize_milestone_bead_ref(&milestone_id, "9ni.8.5.3"),
            "9ni.8.5.3"
        );
        assert_eq!(
            canonicalize_milestone_bead_ref(&milestone_id, "other-ms.bead-2"),
            "other-ms.bead-2"
        );
        assert!(milestone_bead_refs_match(
            &milestone_id,
            "8.5.3",
            "9ni.8.5.3"
        ));
    }

    #[test]
    fn preserves_numeric_dotted_refs_for_numeric_milestone_ids() {
        let milestone_id = MilestoneId::new("10").expect("milestone id");

        assert_eq!(
            canonicalize_milestone_bead_ref(&milestone_id, "8.5.3"),
            "8.5.3"
        );
        assert_eq!(
            canonicalize_milestone_bead_ref(&milestone_id, "10.8.5.3"),
            "10.8.5.3"
        );
        assert!(milestone_bead_refs_match(
            &milestone_id,
            "8.5.3",
            "10.8.5.3"
        ));
        assert!(!milestone_bead_refs_match(
            &milestone_id,
            "8.5.3",
            "11.8.5.3"
        ));
    }

    #[test]
    fn does_not_treat_foreign_qualified_ids_as_local_aliases() {
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");

        assert!(!milestone_bead_refs_match(
            &milestone_id,
            "other-ms.bead-2",
            "ms-alpha.other-ms.bead-2"
        ));
    }

    #[test]
    fn missing_detection_requires_specific_bead_not_found_messages() {
        assert!(br_show_output_indicates_missing(
            "issue not found: ms-alpha.bead-2",
            ""
        ));
        assert!(br_show_output_indicates_missing(
            "",
            "bead not found: ms-alpha.bead-2"
        ));
        assert!(!br_show_output_indicates_missing(
            "permission issue: path not found",
            ""
        ));
    }
}
