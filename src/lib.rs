#![warn(missing_docs, unreachable_pub)]
#![deny(unused_must_use, rust_2018_idioms)]

//! Utilities for long-running, resilient tasks.
//!
//! This library contains code I wrote, found useful, and want to keep using.
//! It aims to provide quick and re-usable scaffolding for building daemons.
//! It is heavily optimized for my preferred dev ergonomics, and is not heavily
//! optimized for performance.
//!
//! The general style is focused on spawning long-lived worker loops, which use
//! channels to communicate.
//!
//! ```no_compile
//! impl Boulder for MyWorkerTask {
//!   fn spawn(self) -> JoinHandle<Fall<Self>> {
//!     tokio::spawn(async move {
//!       while let Some(item) = self.pipe.next().await {
//!         // work happens here :)
//!       }
//!     });
//!   }
//! }
//!
//! let task = MyWorkerTask::new().run_forever();
//! ```

/// Pipe with process-once semantics
pub mod pipe;
/// Resumable, never-ending, tasks
pub mod sisyphus;
/// Crate-internal utils
mod utils;

pub use pipe::{Pipe, PipeError};
pub use sisyphus::{Boulder, Fall, Sisyphus};
