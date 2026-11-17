use std::process::Command;

pub fn release_version() -> Result<(), String> {
    for name in [
        "TUMPEL_RELEASE_VERSION",
        "GITHUB_REF_TYPE",
        "GITHUB_REF_NAME",
        "GITHUB_SHA",
    ] {
        println!("cargo::rerun-if-env-changed={name}");
    }
    let version = match (
        std::env::var("TUMPEL_RELEASE_VERSION"),
        std::env::var("GITHUB_REF_TYPE").as_deref(),
    ) {
        (Ok(version), _) => version,
        (Err(_), _) if packaged() => {
            std::env::var("CARGO_PKG_VERSION").map_err(|error| format!("CARGO_PKG_VERSION is unreadable: {error}"))?
        }
        (Err(_), Ok("tag")) => std::env::var("GITHUB_REF_NAME")
            .map_err(|error| format!("GITHUB_REF_TYPE is tag and GITHUB_REF_NAME is unreadable: {error}"))?,
        (Err(_), _) => {
            let commit = head_commit()?;
            let short = commit
                .get(..12)
                .ok_or_else(|| format!("the commit {commit} is shorter than 12 characters"))?;
            format!("0.0.0-{short}")
        }
    };
    println!("cargo::rustc-env=TUMPEL_RELEASE_VERSION={version}");
    Ok(())
}

fn packaged() -> bool {
    std::env::var_os("CARGO_MANIFEST_DIR")
        .is_some_and(|dir| std::path::Path::new(&dir).join("Cargo.toml.orig").is_file())
}

fn head_commit() -> Result<String, String> {
    if let Ok(commit) = std::env::var("GITHUB_SHA") {
        return Ok(commit);
    }
    println!(
        "cargo::rerun-if-changed={}",
        git(&["rev-parse", "--git-path", "logs/HEAD"])?
    );
    git(&["rev-parse", "HEAD"])
}

fn git(args: &[&str]) -> Result<String, String> {
    let fix = "build inside the repository or set TUMPEL_RELEASE_VERSION";
    let output = Command::new("git")
        .args(args)
        .output()
        .map_err(|error| format!("git {} did not start: {error}; {fix}", args.join(" ")))?;
    if !output.status.success() {
        return Err(format!(
            "git {} failed: {}; {fix}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}
