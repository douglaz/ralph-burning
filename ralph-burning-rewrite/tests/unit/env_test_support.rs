pub static PATH_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub fn lock_path_mutex() -> std::sync::MutexGuard<'static, ()> {
    PATH_MUTEX.lock().unwrap_or_else(|error| error.into_inner())
}

pub struct PathGuard {
    original: Option<std::ffi::OsString>,
}

impl PathGuard {
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
