# Sisyphus

Utilities for long-running, resilient tasks.

This library contains code I wrote, found useful, and want to keep using. It aims to provide systems

The general idiom is focused on spawning long-lived worker loops, which use channels to communicate.

### Understanding this crate

### High Level Example

```rust
pub struct MyWorker{
  pipe: Pipe<WorkItem>,
}

impl Boulder for MyWorker {
  fn spawn(self) -> JoinHandle<Fall<Self>> {
    tokio::spawn(async move {
      // pipe gives refs to items
      while let Some(item) = self.pipe.next().await {
        // worker does work on each item as it becomes available
        self.do_work_on(item);
        // do some async work too :)
        self.async_work_as_well(item).await;
      }
    });
  }
}

let task = MyWorker::new(a_pipe).run_forever();
```

### Current Utils:

- Sisyphus

  - A scaffolding system for spawning long-lived, recoverable tasks
  - A `Boulder` is a looping, fallible task
  - `Boulder` logic is defined in a `Boulder::spawn()` method that returns a
    `JoinHandle`
  - A `Fall` is an error in that task.
    - `Fall::Recoverable` - errors that the task believes it can recover
    - `Fall::Unrecoverable` - errors that the task believes it cannot recover
  - The `Boulder::run_until_panic` method handles restarting on recoverable
    `Fall` s, and reporting unrecoverable errors
  - `Boulder` s may define custom recovery or cleanup logic
  - `Boulder` s may also panic. In that case, no `Fall` is generated, and the
    - panic is propagated upward
  - `Sisyphus` manages a `Boulder` loop. He exposes an interface to observe its
    status and abort the work

- Pipe
  - An inbound and an outbound channel
  - Enforce process-once semantics
  - Prevents data loss on worker error
  - Designed for relatively linear data-processing pipelines
    - e.g. retrieval -> metrics -> indexing -> other handling
  - Convenience methods for running synchronous and asynchronous `for_each` on channel contents

### Future Utils:

- Abstraction layers for instantiating complex pipes from lists of Sisyphuses
- Pipes should allow sync & async transforms (inbound `T`, outbound `U`)

### Copyright Note

Some code descends from utilities written for [Nomad](https://github.com/nomad-xyz/rust/tree/prestwich/monitor/agent-utils). It is used and reproduced under its license terms.
