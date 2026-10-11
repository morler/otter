//! Command-line behaviour: exit codes and where console output goes.

use std::process::{Command, Output};

/// Owns a fresh, uniquely named directory and removes it on drop, so a failing
/// assertion does not leave test artifacts behind.
struct ScratchDir(std::path::PathBuf);

impl ScratchDir {
    fn new(test: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("otter-cli-{}-{nanos}-{test}", std::process::id()));
        // create_dir (not create_dir_all) fails if a path collision already exists.
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Write `source` to a fresh script named after `test` and run otter on it.
fn run_otter(test: &str, source: &str) -> Output {
    let dir = ScratchDir::new(test);
    let script = dir.0.join("script.js");
    std::fs::write(&script, source).unwrap();
    Command::new(env!("CARGO_BIN_EXE_otter"))
        .arg(&script)
        .output()
        .unwrap()
}

#[test]
fn syntax_error_makes_exit_code_non_zero() {
    let out = run_otter("syntax", "let x = ;");
    assert!(!out.status.success());
}

#[test]
fn missing_script_makes_exit_code_non_zero() {
    let out = Command::new(env!("CARGO_BIN_EXE_otter"))
        .arg("/nonexistent/otter-missing.js")
        .output()
        .unwrap();
    assert!(!out.status.success());
}

#[test]
fn console_error_writes_to_stderr_not_stdout() {
    let out = run_otter("console", r#"console.log("ok"); console.error("boom");"#);
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stdout.contains("ok") && !stdout.contains("boom"),
        "stdout: {stdout}"
    );
    assert!(
        stderr.contains("boom") && !stderr.contains("ok"),
        "stderr: {stderr}"
    );
}
