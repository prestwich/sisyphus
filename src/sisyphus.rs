use std::{
    future::IntoFuture,
    panic,
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

#[derive(Debug)]
pub enum Fall<T> {
    Recoverable {
        task: T,
        err: eyre::Report,
    },
    Unrecoverable {
        worth_logging: bool,
        err: eyre::Report,
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
    Stopped(eyre::Report),
    /// Task has panicked
    Panicked,
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskStatus::Starting => write!(f, "Starting"),
            TaskStatus::Running => write!(f, "Running"),
            TaskStatus::Recovering(e) => write!(f, "Restarting:\n{}", e),
            TaskStatus::Stopped(e) => write!(f, "Stopped:\n{}", e),
            TaskStatus::Panicked => write!(f, "Panicked"),
        }
    }
}

/// A wrapper around a task that should run forever.
///
/// It exposes an interface for gracefully shutting down the task, as well as
/// inspecting the task's state.
///
/// Sisyphus tasks follow a simple lifecycle:
/// - Before the task has commenced work it is `Starting`
/// - Once work has commenced it is `Running`
/// - If work was interrupted it goes to 1 of 3 states:
///     - `Recovering(eyre::Report)` - indeicates that the task encountered a
///       recoverable error, and will resume running shortly
///     - `Stopped(eyre::Report)` - indicates that the task encountered an
///       unrecoverable will not resume running.
///     - Panicked - indicates that the task has panicked, and will not resume
///       running
pub struct Sisyphus {
    pub(crate) restarts: Arc<AtomicUsize>,
    // TODO: anything else we want out?
    pub(crate) status: tokio::sync::watch::Receiver<TaskStatus>,
    pub(crate) shutdown: tokio::sync::oneshot::Sender<()>,
    pub(crate) task: JoinHandle<()>,
}

impl Sisyphus {
    /// Issue a shutdown command to the task, allowing it to clean up any state.
    ///
    /// This sends a shutdown command to the relevant task. If this command is
    /// received before an unrecoverable erorr is encountered, then the task
    /// will execute its `cleanup()` function and gracefully exit.
    ///
    /// If an unrecoverable error is encountered before the signal, then the
    /// `cleanup()` will not run.
    ///
    /// ### Returns
    ///
    /// The `JoinHandle` to the task, so it can be awaited (if necessary).
    pub fn shutdown(self) -> JoinHandle<()> {
        let _ = self.shutdown.send(());
        self.task
    }

    /// Cancel the task forcefully. This uses tokio's abort, and does not allow
    /// the task to run any cleanup.
    pub fn abort(self) -> JoinHandle<()> {
        self.task.abort();
        self.task
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

/// Convenience trait for conerting errors to `Fall`
pub trait ErrExt: std::error::Error + Sized + Send + Sync + 'static {
    fn recoverable<Task>(self, task: Task) -> Fall<Task>
    where
        Task: Boulder,
    {
        Fall::Recoverable {
            task,
            err: eyre::eyre!(self),
        }
    }

    fn unrecoverable<Task>(self, worth_logging: bool) -> Fall<Task>
    where
        Task: Boulder,
    {
        Fall::Unrecoverable {
            worth_logging,
            err: eyre::eyre!(self),
        }
    }

    fn log_unrecoverable<Task>(self) -> Fall<Task>
    where
        Task: Boulder,
    {
        self.unrecoverable(true)
    }

    fn silent_unrecoverable<Task>(self) -> Fall<Task>
    where
        Task: Boulder,
    {
        self.unrecoverable(false)
    }
}

impl<T> ErrExt for T where T: std::error::Error + Send + Sync + 'static {}

pub trait Boulder: std::fmt::Display + Sized {
    /// Defaults to 15 seconds. Can be overridden with arbitrary behavior
    fn restart_after_ms(&self) -> u64 {
        15_000
    }

    /// A short description of the task, defaults to Display impl
    fn task_description(&self) -> String {
        format!("{}", self)
    }

    /// Perform the task
    fn spawn(self) -> JoinHandle<Fall<Self>>
    where
        Self: 'static + Send + Sync + Sized;

    /// Clean up the task state
    fn cleanup(&self);

    /// Perform any cleanup required to reboot the task
    fn recover(&self);

    /// Run the task until it panics. Errors result in a task restart with the
    /// same channels. This means that an error causes the task to lose only
    /// the data that is in-scope when it faults.
    fn run_until_panic(self) -> Sisyphus
    where
        Self: 'static + Send + Sync + Sized,
    {
        let task_description = self.task_description();

        let (tx, rx) = watch::channel(TaskStatus::Starting);
        let (shutdown, shutdown_recv) = oneshot::channel();

        let restarts: Arc<AtomicUsize> = Default::default();
        let restarts_loop_ref = restarts.clone();

        let task: JoinHandle<()> = tokio::spawn(async move {
            let handle = self.spawn();
            tokio::pin!(handle);
            tokio::pin!(shutdown_recv);
            loop {
                select! {
                    biased;
                    _ = &mut shutdown_recv => {
                        break;
                    },
                    result = &mut handle => {
                        let again = match result {
                            Ok(Fall::Recoverable { task, err }) => {
                                tracing::warn!(
                                    error = %err,
                                    task = task_description.as_str(),
                                    "Restarting task",
                                );
                                // Sisyphus has been dropped, so we can drop this task
                                if tx.send(TaskStatus::Recovering(err)).is_err() {
                                    break;
                                }
                                task
                            }

                            Ok(Fall::Unrecoverable { err, worth_logging }) => {
                                if worth_logging {
                                    tracing::error!(err = %err, task = task_description.as_str(), "Unrecoverable error encountered");
                                } else {
                                    tracing::trace!(err = %err, task = task_description.as_str(), "Unrecoverable error encountered");
                                }
                                // We don't check the result of the send
                                // because we're stopping regardless of
                                // whether it worked
                                let _ = tx.send(TaskStatus::Stopped(err));
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
                                    let _ = tx.send(TaskStatus::Stopped(eyre::eyre!(panic_res.unwrap_err())));
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

                        // if we haven't broken from within th match, increment
                        // restarts and push the boulder again
                        utils::noisy_sleep(again.restart_after_ms()).await;
                        restarts_loop_ref.fetch_add(1, Ordering::Relaxed);
                        *handle = again.spawn();
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

#[cfg(test)]
pub(crate) mod test {
    use super::*;

    struct RecoverableTask;
    impl std::fmt::Display for RecoverableTask {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RecoverableTask")
        }
    }

    impl Boulder for RecoverableTask {
        fn recover(&self) {}

        fn cleanup(&self) {}

        fn spawn(self) -> JoinHandle<Fall<Self>>
        where
            Self: 'static + Send + Sync + Sized,
        {
            tokio::spawn(async move {
                Fall::Recoverable {
                    task: self,
                    err: eyre::eyre!("This error was recoverable"),
                }
            })
        }
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_recovery() {
        let handle = RecoverableTask.run_until_panic();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let handle = handle.shutdown();
        let result = handle.await;

        assert!(logs_contain("RecoverableTask"));
        assert!(logs_contain("Restarting task"));
        assert!(logs_contain("This error was recoverable"));
        assert!(result.is_ok());
    }

    struct UnrecoverableTask;
    impl std::fmt::Display for UnrecoverableTask {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "UnrecoverableTask")
        }
    }

    impl Boulder for UnrecoverableTask {
        fn recover(&self) {}

        fn cleanup(&self) {}

        fn spawn(self) -> JoinHandle<Fall<Self>>
        where
            Self: 'static + Send + Sync + Sized,
        {
            tokio::spawn(async move {
                Fall::Unrecoverable {
                    err: eyre::eyre!("This error was unrecoverable"),
                    worth_logging: true,
                }
            })
        }
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_unrecoverable() {
        let handle = UnrecoverableTask.run_until_panic();
        let result = handle.await;
        assert!(logs_contain("UnrecoverableTask"));
        assert!(logs_contain("Unrecoverable error encountered"));
        assert!(logs_contain("This error was unrecoverable"));
        assert!(result.is_ok());
    }

    struct PanicTask;
    impl std::fmt::Display for PanicTask {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "PanicTask")
        }
    }

    impl Boulder for PanicTask {
        fn recover(&self) {}

        fn cleanup(&self) {}

        fn spawn(self) -> JoinHandle<Fall<Self>>
        where
            Self: 'static + Send + Sync + Sized,
        {
            tokio::spawn(async move { panic!("intentional panic :)") })
        }
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_panic() {
        let handle = PanicTask.run_until_panic();
        let result = handle.await;
        assert!(logs_contain("PanicTask"));
        assert!(logs_contain("Internal task panicked"));
        assert!(result.is_err() && result.unwrap_err().is_panic());
    }
}
