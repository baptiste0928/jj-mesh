//! jj-mesh CLI. See `lib.rs` for the implementation.

use std::process::ExitCode;

// mimalloc is more efficient for long-running processes and better returns
// memory to the OS than the default glibc's malloc
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> ExitCode {
    match jj_mesh::cli::run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            jj_mesh::cli::error(&err);
            ExitCode::FAILURE
        }
    }
}
