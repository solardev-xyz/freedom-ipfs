//! What this build of freedom-ipfs is, for every host to report alike: the
//! Electron add-on and mobile FFI (`freedom_ipfs_build_info_json`), and
//! freedom-core's service process (`ipfs.hello`).

/// The variables that carry a build's git state. The packaging script
/// (`scripts/package-electron-addon.js`) sets them from `git rev-parse` and
/// `git describe` unless they are set already; any other build can set them
/// too. A build without them reports `None`.
pub const GIT_COMMIT_VAR: &str = "FREEDOM_IPFS_BUILD_GIT_COMMIT";
pub const GIT_DESCRIBE_VAR: &str = "FREEDOM_IPFS_BUILD_GIT_DESCRIBE";

/// A build of freedom-ipfs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuildInfo {
    pub name: &'static str,
    /// The workspace version.
    pub version: &'static str,
    /// The git commit it was built from (`FREEDOM_IPFS_BUILD_GIT_COMMIT`).
    pub git_commit: Option<&'static str>,
    /// `git describe` at build time (`FREEDOM_IPFS_BUILD_GIT_DESCRIBE`).
    pub git_describe: Option<&'static str>,
    pub os: &'static str,
    pub arch: &'static str,
    pub family: &'static str,
}

impl BuildInfo {
    /// The release tag of this version: `v` and the version.
    pub fn release_tag(&self) -> String {
        format!("v{}", self.version)
    }
}

/// This build.
pub fn build_info() -> BuildInfo {
    BuildInfo {
        name: "freedom-ipfs",
        version: env!("CARGO_PKG_VERSION"),
        // `option_env!` takes a literal: the names are GIT_COMMIT_VAR and
        // GIT_DESCRIBE_VAR, which a test holds to these.
        git_commit: option_env!("FREEDOM_IPFS_BUILD_GIT_COMMIT").filter(|v| !v.is_empty()),
        git_describe: option_env!("FREEDOM_IPFS_BUILD_GIT_DESCRIBE").filter(|v| !v.is_empty()),
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        family: std::env::consts::FAMILY,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn this_build() {
        let info = build_info();
        assert_eq!(info.name, "freedom-ipfs");
        assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(
            info.release_tag(),
            format!("v{}", env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(info.os, std::env::consts::OS);
        assert_eq!(
            info.git_commit,
            option_env!("FREEDOM_IPFS_BUILD_GIT_COMMIT").filter(|v| !v.is_empty())
        );
    }

    /// The names read here are the ones the packaging script sets: before,
    /// the code read FREEDOM_IPFS_GIT_* and the script set
    /// FREEDOM_IPFS_BUILD_GIT_*, so the fields were always null.
    #[test]
    fn the_variables_are_the_packaging_scripts() {
        let source = include_str!("build_info.rs");
        let script = include_str!("../../../scripts/package-electron-addon.js");
        for name in [GIT_COMMIT_VAR, GIT_DESCRIBE_VAR] {
            assert!(
                source.contains(&format!("option_env!(\"{name}\")")),
                "{name} isn't what build_info reads"
            );
            assert!(
                script.contains(&format!("env.{name} =")),
                "the packaging script doesn't set {name}"
            );
        }
    }
}
