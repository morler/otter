//! Command-line behaviour: exit codes and where console output goes.

use std::process::{Command, Output};

/// Write `source` to a fresh script named after `test` and run otter on it.
fn run_otter(test: &str, source: &str) -> Output {
    let dir = std::env::temp_dir().join(format!("otter-cli-{}-{test}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let script = dir.join("script.js");
    std::fs::write(&script, source).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_otter")).arg(&script).output().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    out
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
    assert!(stdout.contains("ok") && !stdout.contains("boom"), "stdout: {stdout}");
    assert!(stderr.contains("boom"), "stderr: {stderr}");
}
