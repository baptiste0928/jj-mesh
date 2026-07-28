//! jj-mesh CLI. See `lib.rs` for the implementation.

use std::process::ExitCode;

use jj_mesh::daemon::control::DaemonNotRunning;

fn main() -> ExitCode {
    color_eyre::install().expect("color_eyre installs once");

    match jj_mesh::cli::run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) if err.is::<DaemonNotRunning>() => {
            eprintln!("{}", console::style(format!("{err:#}")).red().for_stderr());
            ExitCode::FAILURE
        }
        Err(err) => {
            eprintln!(
                "{}",
                console::style(format!("Error: {err:#}")).red().for_stderr()
            );
            ExitCode::FAILURE
        }
    }
}
