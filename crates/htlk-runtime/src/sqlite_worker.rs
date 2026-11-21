//! Private asynchronous access to a thread-owned relational connection.
//!
//! Each call runs to completion in FIFO order, even if its awaiting caller leaves.
//! A close message follows already queued work and closes all cloned handles.
//! Database transactions, not this queue, arbitrate independent store connections.
//! The bounded queue admits at most 256 waiting operations per shared handle.

use crate::store::{StorePublicationMetrics, StoreWriteMetrics};
use crate::store_db as rusqlite;
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub(crate) enum Failure<E> {
    Closed,
    Error(E),
    Database(rusqlite::Error),
}
pub(crate) type Error<E> = Failure<E>;

type Operation = Box<dyn FnOnce(&mut rusqlite::Connection) + Send>;

enum Message {
    Call(Operation),
    Close(oneshot::Sender<rusqlite::Result<()>>),
}

#[derive(Clone)]
pub(crate) struct Connection {
    sender: mpsc::Sender<Message>,
    writes: Arc<WriteMetrics>,
    publications: Arc<PublicationMetrics>,
}

#[derive(Default)]
struct PublicationMetrics {
    commits: AtomicU64,
    acquire_ns: AtomicU64,
    hold_ns: AtomicU64,
    statements: AtomicU64,
}

#[derive(Default)]
struct WriteMetrics {
    operations: AtomicU64,
    rows: AtomicU64,
}

impl Connection {
    pub(crate) async fn open(path: impl AsRef<Path>) -> Result<Self, Error<rusqlite::Error>> {
        let path = path.as_ref().to_owned();
        Self::start(move || rusqlite::Connection::open(path)).await
    }

    pub(crate) async fn open_in_memory() -> Result<Self, Error<rusqlite::Error>> {
        Self::start(rusqlite::Connection::open_in_memory).await
    }

    pub(crate) async fn start(
        open: impl FnOnce() -> rusqlite::Result<rusqlite::Connection> + Send + 'static,
    ) -> Result<Self, Error<rusqlite::Error>> {
        let (sender, mut receiver) = mpsc::channel(256);
        let (ready, opened) = oneshot::channel();
        thread::Builder::new()
            .name("htlk-store".into())
            .spawn(move || {
                let mut db = match open() {
                    Ok(db) => db,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                if ready.send(Ok(())).is_err() {
                    return;
                }
                while let Some(message) = receiver.blocking_recv() {
                    match message {
                        Message::Call(operation) => operation(&mut db),
                        Message::Close(reply) => match db.close() {
                            Ok(()) => {
                                let _ = reply.send(Ok(()));
                                return;
                            }
                            Err((connection, error)) => {
                                db = connection;
                                let _ = reply.send(Err(error));
                            }
                        },
                    }
                }
            })
            .map_err(|_| Error::Closed)?;
        opened
            .await
            .map_err(|_| Error::Closed)?
            .map_err(Error::Error)?;
        Ok(Self {
            sender,
            writes: Arc::default(),
            publications: Arc::default(),
        })
    }

    pub(crate) fn write_metrics(&self) -> StoreWriteMetrics {
        StoreWriteMetrics {
            operations: self.writes.operations.load(Ordering::Relaxed),
            rows: self.writes.rows.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn publication_metrics(&self) -> StorePublicationMetrics {
        StorePublicationMetrics {
            commits: self.publications.commits.load(Ordering::Relaxed),
            acquire_ns: self.publications.acquire_ns.load(Ordering::Relaxed),
            hold_ns: self.publications.hold_ns.load(Ordering::Relaxed),
            statements: self.publications.statements.load(Ordering::Relaxed),
        }
    }

    pub(crate) async fn call<R, E>(
        &self,
        operation: impl FnOnce(&mut rusqlite::Connection) -> Result<R, E> + Send + 'static,
    ) -> Result<R, Error<E>>
    where
        R: Send + 'static,
        E: Send + 'static,
    {
        self.call_mode(rusqlite::OperationMode::Exclusive, operation)
            .await
    }

    pub(crate) async fn read<R, E>(
        &self,
        operation: impl FnOnce(&mut rusqlite::Connection) -> Result<R, E> + Send + 'static,
    ) -> Result<R, Error<E>>
    where
        R: Send + 'static,
        E: Send + 'static,
    {
        self.call_mode(rusqlite::OperationMode::Read, operation)
            .await
    }

    pub(crate) async fn write_run<R, E>(
        &self,
        run: String,
        operation: impl FnOnce(&mut rusqlite::Connection) -> Result<R, E> + Send + 'static,
    ) -> Result<R, Error<E>>
    where
        R: Send + 'static,
        E: Send + 'static,
    {
        self.call_mode(rusqlite::OperationMode::Run(run), operation)
            .await
    }

    async fn call_mode<R, E>(
        &self,
        mode: rusqlite::OperationMode,
        operation: impl FnOnce(&mut rusqlite::Connection) -> Result<R, E> + Send + 'static,
    ) -> Result<R, Error<E>>
    where
        R: Send + 'static,
        E: Send + 'static,
    {
        let (reply, result) = oneshot::channel();
        let writes = self.writes.clone();
        #[cfg(feature = "postgres")]
        let publications = self.publications.clone();
        self.sender
            .send(Message::Call(Box::new(move |db| {
                let outcome = match db.begin_operation(mode) {
                    Err(error) => Err(Error::Database(error)),
                    Ok(()) => {
                        let before = db.affected_rows();
                        let outcome = operation(db);
                        match db.finish_operation(outcome.is_ok()) {
                            Err(error) => Err(Error::Database(error)),
                            Ok(()) => {
                                #[cfg(feature = "postgres")]
                                if let Some(sample) = db.publication_sample() {
                                    publications.commits.fetch_add(1, Ordering::Relaxed);
                                    publications
                                        .acquire_ns
                                        .fetch_add(sample.acquire_ns, Ordering::Relaxed);
                                    publications
                                        .hold_ns
                                        .fetch_add(sample.hold_ns, Ordering::Relaxed);
                                    publications
                                        .statements
                                        .fetch_add(sample.statements, Ordering::Relaxed);
                                }
                                let rows = db.affected_rows().saturating_sub(before);
                                if outcome.is_ok() && rows > 0 {
                                    writes.operations.fetch_add(1, Ordering::Relaxed);
                                    writes.rows.fetch_add(rows, Ordering::Relaxed);
                                }
                                outcome.map_err(Error::Error)
                            }
                        }
                    }
                };
                let _ = reply.send(outcome);
            })))
            .await
            .map_err(|_| Error::Closed)?;
        result.await.map_err(|_| Error::Closed)?
    }

    pub(crate) async fn close(self) -> Result<(), Error<rusqlite::Error>> {
        let (reply, result) = oneshot::channel();
        if self.sender.send(Message::Close(reply)).await.is_err() {
            return Ok(());
        }
        // Another cloned handle may already have closed the connection.
        result.await.unwrap_or(Ok(())).map_err(Error::Error)
    }
}
