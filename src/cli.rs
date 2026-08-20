use clap::{ArgGroup, Parser, ValueEnum};
use std::sync::LazyLock;

#[derive(ValueEnum, Clone, Debug, Copy)]
pub enum KeypairType {
    X25519,
    X448,
}

static VERSION: LazyLock<&'static str> =
    LazyLock::new(|| option_env!("VERGEN_GIT_DESCRIBE").unwrap_or(env!("VERGEN_BUILD_SEMVER")));
static LONG_VERSION: LazyLock<String> = LazyLock::new(|| {
    format!(
        "
Build Timestamp:     {}
Build Version:       {}
Commit SHA:          {:?}
Commit Date:         {:?}
Commit Branch:       {:?}
cargo Target Triple: {}
cargo Profile:       {}
cargo Features:      {}
",
        env!("VERGEN_BUILD_TIMESTAMP"),
        env!("VERGEN_BUILD_SEMVER"),
        option_env!("VERGEN_GIT_SHA"),
        option_env!("VERGEN_GIT_COMMIT_TIMESTAMP"),
        option_env!("VERGEN_GIT_BRANCH"),
        env!("VERGEN_CARGO_TARGET_TRIPLE"),
        env!("VERGEN_CARGO_PROFILE"),
        env!("VERGEN_CARGO_FEATURES")
    )
});

#[derive(Parser, Debug, Default, Clone)]
#[command(
    about,
    version = *VERSION,
    long_version = LONG_VERSION.as_str()
)]
#[command(group(
    ArgGroup::new("cmds")
        .required(true)
        .args(["CONFIG", "genkey"]),
))]
pub struct Cli {
    /// The path to the configuration file
    ///
    /// Running as a client or a server is automatically determined
    /// according to the configuration file.
    #[arg(id = "CONFIG", value_name = "CONFIG")]
    pub config_path: Option<std::path::PathBuf>,

    /// Run as a server
    #[arg(long, short, group = "mode")]
    pub server: bool,

    /// Run as a client
    #[arg(long, short, group = "mode")]
    pub client: bool,

    /// Generate a keypair for the use of the noise protocol
    ///
    /// The DH function to use is x25519
    #[arg(
        long,
        value_enum,
        value_name = "CURVE",
        num_args = 0..=1,
        default_missing_value = "x25519"
    )]
    pub genkey: Option<Option<KeypairType>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_is_valid() {
        Cli::command().debug_assert();
    }
}
