#![warn(missing_docs, unreachable_pub)]
#![deny(unused_must_use, rust_2018_idioms)]

//! Utilities for long-running, resilient tasks.
//!
//! This library contains code I wrote, found useful, and want to keep using. It aims to provide systems
//!
//! The general idiom is focused on spawning long-lived worker loops, which use channels to communicate.
//!
//! ```no_compile
//! tokio::spawn(
//!   async move {
//!     while let Some(item) = pipe.next().await {
//!       // work happens here :)
//!     }
//!   }
//! );
//! ```

/// Pipe with process-once semantics
pub mod pipe;
/// Resumable, never-ending, tasks
pub mod sisyphus;
/// Crate utils
mod utils;
