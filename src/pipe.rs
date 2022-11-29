use tokio::sync::mpsc;

pub enum PipeError {
    InboundGone,
    OutboundGone,
}

/// A pipe that enforces process-once semantics.
///
/// This type ensures that the pipe's owner tasks sees a piece of data exactly
/// once, and that errors or panics in an owning task do not cause data loss
/// from the pipe.
///
/// Using a pipe has a number of advantages
/// - Data is not lost if errors occur during processing
/// - Data is not process twice if errors occur during processing
/// - Flow control via backpressure is preserved
#[derive(Debug)]
pub struct Pipe<T>
where
    T: std::fmt::Debug + Send + Sync + 'static,
{
    rx: mpsc::Receiver<T>,
    tx: mpsc::Sender<T>,
    contents: Option<T>,
}

impl<T> Pipe<T>
where
    T: std::fmt::Debug + Send + Sync + 'static,
{
    pub fn new(rx: mpsc::Receiver<T>, tx: mpsc::Sender<T>, contents: Option<T>) -> Self {
        Self { rx, tx, contents }
    }

    /// Creates a series of linked pipes, with an inbound channel, and an
    /// outbound channel. This allows you to quickly instantiate an ordered
    /// data-processing pipeline.
    ///
    /// Pipes should be pulled from the pipeline in order, and passed to
    /// processing tasks.
    ///
    /// ## Note
    ///
    /// All pipes must polled in a loop. Otherwise data will not reach
    /// downstream pipes.
    ///
    /// In order to avoid saturating the pipes, the `rx` must be read from.
    /// Otherwise, processing will stop once the total capcity is reached
    pub fn unterminated_pipeline(
        length: usize,
        total_capacity: Option<usize>,
    ) -> (mpsc::Sender<T>, Vec<Pipe<T>>, mpsc::Receiver<T>) {
        let total_capacity = total_capacity.unwrap_or(length * 20);
        let buffer = std::cmp::max(total_capacity / length, 1);
        let (tx, mut rx) = mpsc::channel::<T>(buffer);

        let mut pipeline = Vec::with_capacity(length);

        (0..length).for_each(|_| {
            let (next_tx, mut next_rx) = mpsc::channel::<T>(buffer);
            std::mem::swap(&mut rx, &mut next_rx);
            pipeline.push(Pipe::new(next_rx, next_tx, None));
        });

        (tx, pipeline, rx)
    }

    /// Creates a series of linked pipes, with an inbound channel, and an
    /// outbound channel. This allows you to quickly instantiate an ordered
    /// data-processing pipeline.
    ///
    /// The outbound channel from this pipeline terminates in a simple task
    /// that drops messages as soon as they reach it. This ensures that the
    /// pipeline does not saturate. This task will end as soon as its upstream
    /// pipes close.
    ///
    /// Pipes should be pulled from the pipeline in order, and passed to
    /// processing tasks.
    ///
    /// ## Note
    ///
    /// All pipes must polled in a loop. Otherwise data will not reach
    /// downstream pipes.
    pub fn pipeline(
        length: usize,
        total_capacity: Option<usize>,
    ) -> (mpsc::Sender<T>, Vec<Pipe<T>>) {
        let (tx, pipeline, mut rx) = Self::unterminated_pipeline(length, total_capacity);
        tokio::spawn(async move {
            loop {
                if rx.recv().await.is_none() {
                    break;
                }
            }
        });
        (tx, pipeline)
    }

    /// Take the contents of the pipe, if any. This prevents them from being
    /// sent out of the pipe, and can be used to filter a value
    pub fn take(&mut self) -> Option<T> {
        self.contents.take()
    }

    /// Read the value in the pipe, if any, without advancing to the next
    /// value. Typically tasks should use `next()` to wait for the next value.
    /// Read may be used to inspect contents without a mutable ref
    pub fn read(&self) -> Option<&T> {
        self.contents.as_ref()
    }

    /// Get an owned copy of the contents, without advancing to the next value
    pub fn to_owned(&self) -> Option<<T as ToOwned>::Owned>
    where
        T: ToOwned,
    {
        self.read().map(|c| c.to_owned())
    }

    /// Reserve channel capacity, and then move the contents into the channel
    async fn send(&mut self) -> Result<(), PipeError> {
        if self.contents.is_some() {
            let permit = self
                .tx
                .reserve()
                .await
                .map_err(|_| PipeError::OutboundGone)?;
            permit.send(self.contents.take().unwrap());
        }
        Ok(())
    }

    /// Release the current contents of the pipe and wait for the next value to
    /// become available.
    ///
    /// # Cancel Safety
    ///
    /// Because the pipe owns the data, and the owner of the pipe only borrows
    /// it, data is preserved through cancellation. At any await point in the
    /// `next()` future, one of the following is true:
    ///
    /// - The pipe still owns the data.
    /// - The pipe has sent the data onwards, and is currently empty.
    ///
    /// If another task takes ownership of the pipe and resumes work, the
    /// message will not be seen again.
    pub async fn next(&mut self) -> Result<&T, PipeError> {
        self.send().await?;
        let next = self.rx.recv().await.ok_or(PipeError::InboundGone)?;
        self.contents = Some(next);
        Ok(self.read().expect("checked"))
    }

    /// Converts a pipeline to a task that simply polls `next()`
    pub fn nop(self) {
        self.for_each(|_| {});
    }

    /// Run a synchronous function on each element
    ///
    /// This will run indefinitely, until an upstream or downstream channel
    /// closes.
    pub fn for_each<Func>(mut self, f: Func)
    where
        Func: Fn(&T) + Send + 'static,
    {
        tokio::spawn(async move {
            while let Ok(contents) = self.next().await {
                f(contents);
            }
        });
    }

    /// Run an async function on each element. Does not distinguish between
    /// success and failure of that function.
    ///
    /// This will run indefinitely, until an upstream or downstream channel
    /// closes.
    ///
    /// # Note:
    ///
    /// Be careful not to bottleneck your pipeline :) Consider adding a timeout
    /// wrapper to your async function
    pub fn for_each_async<Func, Fut, Out>(mut self, f: Func)
    where
        Func: Fn(&T) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Out> + Send + 'static,
    {
        tokio::spawn(async move {
            while let Ok(contents) = self.next().await {
                f(contents).await;
            }
        });
    }
}

impl<T> Drop for Pipe<T>
where
    T: std::fmt::Debug + Send + Sync + 'static,
{
    fn drop(&mut self) {
        // we attempt to empty the contents on drop, knowing that some
        // downstream task may still be running.
        if let Some(contents) = self.contents.take() {
            let _ = self.tx.try_send(contents);
        }
    }
}
