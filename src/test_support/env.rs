//! Environment and process-wide guards for deterministic tests.
//!
//! # Examples
//!
//! ```ignore
//! use ralph_burning::test_support::env::{lock_path_mutex, PathGuard};
//!
//! let _lock = lock_path_mutex();
//! let _guard = PathGuard::replace(std::path::Path::new("/tmp/fake-bin"));
//! ```

/// Serializes tests that mutate `PATH`.
pub static PATH_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Acquire the global `PATH` mutation lock.
pub fn lock_path_mutex() -> std::sync::MutexGuard<'static, ()> {
    PATH_MUTEX.lock().unwrap_or_else(|error| error.into_inner())
}

/// Serializes tests that mutate `OPENROUTER_API_KEY`.
pub static OPENROUTER_KEY_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Acquire the global `OPENROUTER_API_KEY` mutation lock.
pub fn lock_openrouter_key_mutex() -> std::sync::MutexGuard<'static, ()> {
    OPENROUTER_KEY_MUTEX
        .lock()
        .unwrap_or_else(|error| error.into_inner())
}

/// Restores the original `OPENROUTER_API_KEY` when dropped.
pub struct OpenRouterKeyGuard {
    original: Option<String>,
}

impl OpenRouterKeyGuard {
    /// Remove `OPENROUTER_API_KEY` for the lifetime of this guard.
    ///
    /// ```ignore
    /// use ralph_burning::test_support::env::{lock_openrouter_key_mutex, OpenRouterKeyGuard};
    ///
    /// let _lock = lock_openrouter_key_mutex();
    /// let _guard = OpenRouterKeyGuard::remove();
    /// ```
    pub fn remove() -> Self {
        let original = std::env::var("OPENROUTER_API_KEY").ok();
        std::env::remove_var("OPENROUTER_API_KEY");
        Self { original }
    }

    /// Set `OPENROUTER_API_KEY` for the lifetime of this guard.
    ///
    /// ```ignore
    /// use ralph_burning::test_support::env::{lock_openrouter_key_mutex, OpenRouterKeyGuard};
    ///
    /// let _lock = lock_openrouter_key_mutex();
    /// let _guard = OpenRouterKeyGuard::set("test-key");
    /// ```
    pub fn set(value: &str) -> Self {
        let original = std::env::var("OPENROUTER_API_KEY").ok();
        std::env::set_var("OPENROUTER_API_KEY", value);
        Self { original }
    }
}

impl Drop for OpenRouterKeyGuard {
    fn drop(&mut self) {
        match &self.original {
            Some(value) => std::env::set_var("OPENROUTER_API_KEY", value),
            None => std::env::remove_var("OPENROUTER_API_KEY"),
        }
    }
}

/// Restores the original `PATH` when dropped.
pub struct PathGuard {
    original: Option<std::ffi::OsString>,
}

impl PathGuard {
    /// Prepend a directory to `PATH` for the lifetime of this guard.
    ///
    /// ```ignore
    /// use ralph_burning::test_support::env::{lock_path_mutex, PathGuard};
    ///
    /// let _lock = lock_path_mutex();
    /// let _guard = PathGuard::prepend(std::path::Path::new("/tmp/fake-bin"));
    /// ```
    pub fn prepend(dir: &std::path::Path) -> Self {
        let original = std::env::var_os("PATH");
        let new_path = match &original {
            Some(existing) => {
                let mut paths = std::env::split_paths(existing).collect::<Vec<_>>();
                paths.insert(0, dir.to_path_buf());
                std::env::join_paths(paths).expect("join paths")
            }
            None => dir.as_os_str().to_owned(),
        };
        std::env::set_var("PATH", &new_path);
        Self { original }
    }

    /// Replace `PATH` for the lifetime of this guard.
    ///
    /// ```ignore
    /// use ralph_burning::test_support::env::{lock_path_mutex, PathGuard};
    ///
    /// let _lock = lock_path_mutex();
    /// let _guard = PathGuard::replace(std::path::Path::new("/tmp/fake-bin"));
    /// ```
    pub fn replace(dir: &std::path::Path) -> Self {
        let original = std::env::var_os("PATH");
        std::env::set_var("PATH", dir);
        Self { original }
    }
}

impl Drop for PathGuard {
    fn drop(&mut self) {
        match &self.original {
            Some(value) => std::env::set_var("PATH", value),
            None => std::env::remove_var("PATH"),
        }
    }
}
