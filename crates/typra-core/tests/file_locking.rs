use std::process::Command;

#[test]
fn second_writer_fails_fast_and_read_only_shared_lock_blocks_while_writer_active() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.typra");

    // First writer opens and holds lock for the duration of the test.
    let _db = typra_core::Database::open(&path).unwrap();

    // Same-process read-only can open (we avoid taking an overlapping shared lock when the
    // writer lock is already held by this process).
    let _ro_same_process = typra_core::Database::open_read_only(&path).unwrap();

    // A read-only open in another process should fail while a writer holds the lock.
    let exe = std::env::current_exe().unwrap();
    let out_ro = Command::new(&exe)
        .env("TYPRA_LOCKING_CHILD", "1")
        .env("TYPRA_LOCK_PATH", path.to_str().unwrap())
        .arg("--exact")
        .arg("_child_read_only_open")
        .arg("--nocapture")
        .output()
        .unwrap();
    assert!(!out_ro.status.success());

    // A second writer should fail fast. We verify in a child process so we aren't sharing
    // any accidental in-process state.
    let out = Command::new(exe)
        .env("TYPRA_LOCKING_CHILD", "1")
        .env("TYPRA_LOCK_PATH", path.to_str().unwrap())
        .arg("--exact")
        .arg("_child_writer_open")
        .arg("--nocapture")
        .output()
        .unwrap();
    assert!(!out.status.success());
}

// When invoked as a subprocess, attempt to open as writer.
#[test]
fn _child_writer_open() {
    if std::env::var("TYPRA_LOCKING_CHILD").ok().as_deref() != Some("1") {
        return;
    }
    let path = std::env::var("TYPRA_LOCK_PATH").unwrap();
    let res = typra_core::Database::open(&path);
    if res.is_ok() {
        std::process::exit(0);
    }
    std::process::exit(2);
}

// When invoked as a subprocess, attempt to open as read-only.
#[test]
fn _child_read_only_open() {
    if std::env::var("TYPRA_LOCKING_CHILD").ok().as_deref() != Some("1") {
        return;
    }
    let path = std::env::var("TYPRA_LOCK_PATH").unwrap();
    let res = typra_core::Database::open_read_only(&path);
    if res.is_ok() {
        std::process::exit(0);
    }
    std::process::exit(2);
}
