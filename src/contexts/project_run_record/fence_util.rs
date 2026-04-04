#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FenceDelimiter {
    pub(crate) marker: char,
    pub(crate) count: usize,
}

pub(crate) fn opening_fence_delimiter(line: &str) -> Option<FenceDelimiter> {
    let trimmed = line.trim();
    let marker = trimmed.chars().next()?;
    if marker != '`' && marker != '~' {
        return None;
    }

    let count = trimmed.chars().take_while(|ch| *ch == marker).count();
    if count < 3 {
        return None;
    }

    Some(FenceDelimiter { marker, count })
}

pub(crate) fn closes_fence(line: &str, opening: FenceDelimiter) -> bool {
    let Some(candidate) = opening_fence_delimiter(line) else {
        return false;
    };

    if candidate.marker != opening.marker || candidate.count < opening.count {
        return false;
    }

    let trimmed = line.trim();
    trimmed[candidate.count..].trim().is_empty()
}
