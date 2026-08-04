//! jj-mesh CLI. See `lib.rs` for the implementation.

use std::process::ExitCode;

fn main() -> ExitCode {
    color_eyre::install().expect("color_eyre installs once");

    match jj_mesh::cli::run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            jj_mesh::cli::report_error(&err);
            ExitCode::FAILURE
        }
    }
}
