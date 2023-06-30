use std::{
    future::{Future, IntoFuture},
    panic,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use tokio::{
    select,
    sync::{oneshot, watch},
    task::JoinHandle,
};

use crate::utils;

/// An error when pushing a `Boulder`
///
/// ## Recoverability
///
/// [`Boulder`]s explicitly mark themselves as `Recoverable` or `Unrecoverable`
/// and further mark unrecoverable errors as `exceptional`, and no outside
/// runner is required to guess or attempt to handle errors.
///
/// ## Tracing
///
/// Recoverable errors will be traced at `DEBUG`. These are considered normal
/// program execution, and indicate temporary failures like a rate-limit
///
/// Exceptional unrecoverable errors will be traced at `ERROR` level, while
/// unexceptional errors will be traced at `TRACE`. Unexceptional errors are
/// typically program lifecycle events. E.g. a task cancellation, shutdown
/// signal, upstream or downstream pipe failure (indicating another task has
/// permanently dropped its pipe), &c.
#[derive(Debug)]
pub enum Fall<T> {
    /// A recoverable issue
    Recoverable {
        /// The task that triggered the issue, for re-spawning
        task: T,
        /// The issue that triggered the fall
        err: eyre::Report,
        /// The shutdown channel, for gracefully shutting down the task
        shutdown_recv: oneshot::Receiver<()>,
    },
    /// An unrecoverable issue
    Unrecoverable {
        /// Whether it should be considered exceptional.
        exceptional: bool,
        /// The issue that triggered the fall
        err: eyre::Report,
        /// The task that triggered the issue
        task: T,
    },
    /// The signal for shutting down the task has been sent
    Shutdown {
        /// The task that triggered the issue
        task: T,
    },
}

/// The current state of a Sisyphus task.
#[derive(Debug)]
pub enum TaskStatus {
    /// Task is starting
    Starting,
    /// Task is running
    Running,
    /// Task is waiting to resume running
    Recovering(eyre::Report),
    /// Task is stopped, and will not resume
    Stopped {
        /// Whether the error is exceptional, or normal lifecycle
        exceptional: bool,
        /// The error that triggered the stop
        err: eyre::Report,
    },
    /// Task has panicked
    Panicked,
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskStatus::Starting => write!(f, "Starting"),
            TaskStatus::Running => write!(f, "Running"),
            TaskStatus::Recovering(e) => write!(f, "Restarting:\n{e}"),
            TaskStatus::Stopped { exceptional, err } => write!(
                f,
                "Stopped:\n{}{}",
                if *exceptional { "exceptional\n" } else { "" },
                err,
            ),
            TaskStatus::Panicked => write!(f, "Panicked"),
        }
    }
}

/// A wrapper around a task that should run forever.
///
/// It exposes an interface for gracefully shutting down the task, as well as
/// inspecting the task's state. Sisyphus tasks do NOT produce an output. If you
/// would like to extract data from the task, make sure that your `Boulder`
/// includes a channel
///
/// ### Lifecycle
///
/// Sisyphus tasks follow a simple lifecycle:
/// - Before the task has commenced work it is `Starting`
/// - Once work has commenced it is `Running`
/// - If work was interrupted it goes to 1 of 3 states:
///     - `Recovering(eyre::Report)` - indeicates that the task encountered a
///       recoverable error, and will resume running shortly
///     - `Stopped(eyre::Report)` - indicates that the task encountered an
///       unrecoverable will not resume running.
///     - `Panicked` - indicates that the task has panicked, and will not resume
///       running
///
/// ### Why `eyre::Report`? Why not an associated `Error` type?
///
/// A [`Boulder`] is opaque to the environment relying on it. Its lifecycle
/// should be managed by its internal crash+recovery loop. Associated error
/// types add significant complexity to the management system (e.g. adding an
/// error output would require a generic trait bound as follows:
/// `Sisyphus<T: Boulder> { _phantom: PhantomData<T>}`
///
/// To avoid code complexity AND prevent developers from interfering in the
/// lifecycle of the task, we do not allow easy error handling. In other words,
/// errors are intended to be either ignored or traced, never handled. Because
/// its errors are not intended to be handled, we do not expose them to the
/// outside world.
pub struct Sisyphus {
    pub(crate) restarts: Arc<AtomicUsize>,
    // TODO: anything else we want out?
    pub(crate) status: tokio::sync::watch::Receiver<TaskStatus>,
    pub(crate) shutdown: tokio::sync::oneshot::Sender<()>,
    pub(crate) task: JoinHandle<()>,
}

impl Sisyphus {
    /// Issue a shutdown command to the task.
    ///
    /// This sends a shutdown command to the relevant task.
    ///
    /// ### Returns
    ///
    /// The `JoinHandle` to the task, so it can be awaited (if necessary).
    pub fn shutdown(self) -> JoinHandle<()> {
        let _ = self.shutdown.send(());
        self.task
    }

    /// Wait for the task to change status.
    /// Errors if the status channel is closed.
    pub async fn watch_status(&mut self) -> Result<(), watch::error::RecvError> {
        self.status.changed().await
    }

    /// Return the task's current status
    pub fn status(&self) -> String {
        self.status.borrow().to_string()
    }

    /// The number of times the task has restarted
    pub fn restarts(&self) -> usize {
        self.restarts.load(Ordering::Relaxed)
    }
}

impl IntoFuture for Sisyphus {
    type Output = <JoinHandle<()> as IntoFuture>::Output;

    type IntoFuture = <JoinHandle<()> as IntoFuture>::IntoFuture;

    fn into_future(self) -> Self::IntoFuture {
        self.task
    }
}

/// Convenience trait for conerting errors to [`Fall`]
pub trait ErrExt: std::error::Error + Sized + Send + Sync + 'static {
    /// Convert an error to a recoverable [`Fall`]
    fn recoverable<Task>(self, task: Task, shutdown_recv: oneshot::Receiver<()>) -> Fall<Task>
    where
        Task: Boulder,
    {
        Fall::Recoverable {
            task,
            shutdown_recv,
            err: eyre::eyre!(self),
        }
    }

    /// Convert an error to an unrecoverable [`Fall`]
    fn unrecoverable<Task>(self, task: Task, exceptional: bool) -> Fall<Task>
    where
        Task: Boulder,
    {
        Fall::Unrecoverable {
            exceptional,
            err: eyre::eyre!(self),
            task,
        }
    }

    /// Convert an error to an exceptional, unrecoverable [`Fall`]
    fn log_unrecoverable<Task>(self, task: Task) -> Fall<Task>
    where
        Task: Boulder,
    {
        self.unrecoverable(task, true)
    }

    /// Convert an error to an unexcpetional, unrecoverable [`Fall`]
    fn silent_unrecoverable<Task>(self, task: Task) -> Fall<Task>
    where
        Task: Boulder,
    {
        self.unrecoverable(task, false)
    }
}

impl<T> ErrExt for T where T: std::error::Error + Send + Sync + 'static {}

/// A looping, fallible task
pub trait Boulder: std::fmt::Display + Sized {
    /// Defaults to 15 seconds. Can be overridden with arbitrary behavior
    fn restart_after_ms(&self) -> u64 {
        15_000
    }

    /// A short description of the task, defaults to Display impl
    fn task_description(&self) -> String {
        format!("{self}")
    }

    /// Returns true if this is the first time the task has run
    fn first_time(&self, restarts: &Arc<AtomicUsize>) -> bool {
        if restarts.load(Ordering::Relaxed) == 0 {
            true
        } else {
            false
        }
    }

    /// Perform the task
    fn spawn(self, shutdown: oneshot::Receiver<()>) -> JoinHandle<Fall<Self>>
    where
        Self: 'static + Send + Sync + Sized;

    /// Bootstrap the task state. This method will be called before the task spawn.
    ///
    /// Override this function if your task needs to to boostrap its state before
    /// running spawn
    fn bootstrap(&mut self, _first_time: bool) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {})
    }

    /// Clean up the task state. This method will be called by the loop when
    /// the task is shutting down due to an unrecoverable error
    ///
    /// Override this function if your task needs to clean up resources on
    /// an unrecoverable error
    fn cleanup(&mut self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {})
    }

    /// Perform any work required to reboot the task. This method will be
    /// called by the loop when the task has encountered a recoverable error.
    ///
    /// Override this function if your task needs to adjust its state when
    /// hitting a recoverable error
    fn recover(&mut self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {})
    }

    /// Run the task until it panics. Errors result in a task restart with the
    /// same channels. This means that an error causes the task to lose only
    /// the data that is in-scope when it faults.
    fn run_until_panic(mut self) -> Sisyphus
    where
        Self: 'static + Send + Sync + Sized,
    {
        let task_description = self.task_description();

        let (tx, rx) = watch::channel(TaskStatus::Starting);
        let (shutdown, shutdown_recv) = oneshot::channel();

        let restarts: Arc<AtomicUsize> = Default::default();
        let restarts_loop_ref = restarts.clone();

        let task: JoinHandle<()> = tokio::spawn(async move {
            tx.send(TaskStatus::Running)
                .expect("Failed to send task status");
            self.bootstrap(self.first_time(&restarts_loop_ref)).await;
            let handle = self.spawn(shutdown_recv);
            tokio::pin!(handle);
            loop {
                select! {
                    result = &mut handle => {
                        let (again, shutdown_recv) = match result {
                            Ok(Fall::Recoverable { mut task, shutdown_recv, err }) => {
                                // Sisyphus has been dropped, so we can drop this task
                                let e_string = err.to_string();
                                if tx.send(TaskStatus::Recovering(err)).is_err() {
                                    break;
                                }
                                task.recover().await;
                                tracing::debug!(
                                    error = e_string.to_string(),
                                    task = task_description.as_str(),
                                    "Restarting task",
                                );
                                (task, shutdown_recv)
                            }

                            Ok(Fall::Unrecoverable { err, exceptional, mut task }) => {
                                if exceptional {
                                    tracing::error!(err = %err, task = task_description.as_str(), "Unrecoverable error encountered");
                                } else {
                                    tracing::trace!(err = %err, task = task_description.as_str(), "Unrecoverable error encountered");
                                }
                                task.cleanup().await;
                                // We don't check the result of the send
                                // because we're stopping regardless of
                                // whether it worked
                                let _ = tx.send(TaskStatus::Stopped{exceptional, err});
                                break;
                            }

                            Ok(Fall::Shutdown{mut task}) => {
                                task.cleanup().await;
                                // We don't check the result of the send
                                // because we're stopping regardless of
                                // whether it worked
                                let _ = tx.send(TaskStatus::Stopped{exceptional: false, err: eyre::eyre!("Shutdown")});
                                handle.abort();
                                break;
                            }

                            Err(e) => {
                                let panic_res = e.try_into_panic();

                                if panic_res.is_err() {
                                    tracing::trace!(
                                        task = task_description.as_str(),
                                        "Internal task cancelled",
                                    );
                                    // We don't check the result of the send
                                    // because we're stopping regardless of
                                    // whether it worked
                                    let status = TaskStatus::Stopped{
                                        exceptional: false,
                                        err:eyre::eyre!(panic_res.unwrap_err())
                                    };
                                    let _ = tx.send(status);
                                    break;
                                }
                                // We don't check the result of the send
                                // because we're stopping regardless of
                                // whether it worked
                                let _ = tx.send(TaskStatus::Panicked);
                                let p = panic_res.unwrap();
                                tracing::error!(task = task_description.as_str(), "Internal task panicked");
                                panic::resume_unwind(p);
                            }
                        };

                        // We use a noisy sleep here to nudge tasks off
                        // eachother if they're crashing around the same time
                        utils::noisy_sleep(again.restart_after_ms()).await;
                        // If we haven't broken from within the match, increment
                        // restarts and push the boulder again.
                        restarts_loop_ref.fetch_add(1, Ordering::Relaxed);
                        *handle = again.spawn(shutdown_recv);
                    },
                }
            }
        });
        Sisyphus {
            restarts,
            status: rx,
            shutdown,
            task,
        }
    }
}

// #[cfg(test)]
// pub(crate) mod test {
//     use super::*;

//     struct RecoverableTask;
//     impl std::fmt::Display for RecoverableTask {
//         fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//             write!(f, "RecoverableTask")
//         }
//     }

//     impl Boulder for RecoverableTask {
//         fn recover(&mut self) {}

//         fn cleanup(&mut self) {}

//         fn spawn(self) -> JoinHandle<Fall<Self>>
//         where
//             Self: 'static + Send + Sync + Sized,
//         {
//             tokio::spawn(async move {
//                 Fall::Recoverable {
//                     task: self,
//                     err: eyre::eyre!("This error was recoverable"),
//                 }
//             })
//         }
//     }

//     #[tokio::test]
//     #[tracing_test::traced_test]
//     async fn test_recovery() {
//         let handle = RecoverableTask.run_until_panic();
//         tokio::time::sleep(std::time::Duration::from_millis(200)).await;
//         let handle = handle.shutdown();
//         let result = handle.await;

//         assert!(logs_contain("RecoverableTask"));
//         assert!(logs_contain("Restarting task"));
//         assert!(logs_contain("This error was recoverable"));
//         assert!(result.is_ok());
//     }

//     struct UnrecoverableTask;
//     impl std::fmt::Display for UnrecoverableTask {
//         fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//             write!(f, "UnrecoverableTask")
//         }
//     }

//     impl Boulder for UnrecoverableTask {
//         fn recover(&mut self) {}

//         fn cleanup(&mut self) {}

//         fn spawn(self) -> JoinHandle<Fall<Self>>
//         where
//             Self: 'static + Send + Sync + Sized,
//         {
//             tokio::spawn(async move {
//                 Fall::Unrecoverable {
//                     err: eyre::eyre!("This error was unrecoverable"),
//                     exceptional: true,
//                 }
//             })
//         }
//     }

//     #[tokio::test]
//     #[tracing_test::traced_test]
//     async fn test_unrecoverable() {
//         let handle = UnrecoverableTask.run_until_panic();
//         let result = handle.await;
//         assert!(logs_contain("UnrecoverableTask"));
//         assert!(logs_contain("Unrecoverable error encountered"));
//         assert!(logs_contain("This error was unrecoverable"));
//         assert!(result.is_ok());
//     }

//     struct PanicTask;
//     impl std::fmt::Display for PanicTask {
//         fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//             write!(f, "PanicTask")
//         }
//     }

//     impl Boulder for PanicTask {
//         fn recover(&mut self) {}

//         fn cleanup(&mut self) {}

//         fn spawn(self) -> JoinHandle<Fall<Self>>
//         where
//             Self: 'static + Send + Sync + Sized,
//         {
//             tokio::spawn(async move { panic!("intentional panic :)") })
//         }
//     }

//     #[tokio::test]
//     #[tracing_test::traced_test]
//     async fn test_panic() {
//         let handle = PanicTask.run_until_panic();
//         let result = handle.await;
//         assert!(logs_contain("PanicTask"));
//         assert!(logs_contain("Internal task panicked"));
//         assert!(result.is_err() && result.unwrap_err().is_panic());
//     }
// }
