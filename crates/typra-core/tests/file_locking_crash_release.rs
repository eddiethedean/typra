use std::process::Command;

#[test]
fn writer_lock_is_released_after_abrupt_child_exit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.typra");

    // Spawn a child process that opens as writer and aborts.
    let exe = std::env::current_exe().unwrap();
    let out = Command::new(exe)
        .env("TYPRA_LOCKING_ABORT_CHILD", "1")
        .env("TYPRA_LOCK_PATH", path.to_str().unwrap())
        .arg("--exact")
        .arg("_child_open_and_abort")
        .arg("--nocapture")
        .output()
        .unwrap();
    // The child should have exited non-zero because it aborted.
    assert!(!out.status.success());

    // After the child is gone, a writer open should succeed.
    let _db = typra_core::Database::open(&path).unwrap();
}

#[test]
fn _child_open_and_abort() {
    if std::env::var("TYPRA_LOCKING_ABORT_CHILD")
        .ok()
        .as_deref()
        != Some("1")
    {
        return;
    }
    let path = std::env::var("TYPRA_LOCK_PATH").unwrap();
    let _db = typra_core::Database::open(&path).unwrap();
    std::process::abort();
}

