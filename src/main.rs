//! jj-mesh CLI. See `lib.rs` for the implementation.

fn main() -> color_eyre::eyre::Result<()> {
    color_eyre::install()?;
    jj_mesh::cli::run()
}
