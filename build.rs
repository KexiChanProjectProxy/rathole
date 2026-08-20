use anyhow::Result;
use vergen_gitcl::{Build, Cargo, Emitter, Gitcl};

fn main() -> Result<()> {
    // Preserve env vars used by clap long_version that vergen 10 no longer emits.
    println!(
        "cargo:rustc-env=VERGEN_BUILD_SEMVER={}",
        env!("CARGO_PKG_VERSION")
    );
    println!(
        "cargo:rustc-env=VERGEN_CARGO_PROFILE={}",
        std::env::var("PROFILE").unwrap()
    );

    let build = Build::all_build();
    let cargo = Cargo::all_cargo();
    let gitcl = Gitcl::all_git();

    Emitter::default()
        .add_instructions(&build)?
        .add_instructions(&cargo)?
        .add_instructions(&gitcl)?
        .emit()?;
    Ok(())
}
