//! Storage-independent durable transactional outbox (`#lzdurableoutbox`).

use std::collections::BTreeMap;

use crate::ipc::IpcMessage;

/// Sender-side at-least-once outbox contract (spec § DurableOutbox).
pub trait DurableOutbox {
    fn append(&mut self, epoch: u64, msg: IpcMessage);
    fn ack_through(&mut self, epoch: u64);
    fn replay_from(&self, cursor: u64) -> Vec<(u64, IpcMessage)>;
    fn retained_epochs(&self) -> Vec<u64>;
}

/// Dumb ordered byte storage. Retention, cursors, serialization, and replay ordering
/// belong to [`Outbox`], so persistent bindings only implement these five operations.
pub trait OutboxStore {
    fn put(&mut self, epoch: u64, frame: &[u8]);
    fn delete_through(&mut self, epoch: u64);
    fn scan_after(&self, cursor: u64) -> Vec<(u64, Vec<u8>)>;
    fn load_cursor(&self) -> u64;
    fn save_cursor(&mut self, epoch: u64);
}

/// The single ack/prune/replay protocol shared by every storage backend.
pub struct Outbox<S: OutboxStore> {
    store: S,
    acked_through: u64,
}

impl<S: OutboxStore> Outbox<S> {
    pub fn with_store(store: S) -> Self {
        let acked_through = store.load_cursor();
        Self {
            store,
            acked_through,
        }
    }

    pub fn acked_through(&self) -> u64 {
        self.acked_through.max(self.store.load_cursor())
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    pub fn into_store(self) -> S {
        self.store
    }
}

impl<S: OutboxStore> DurableOutbox for Outbox<S> {
    fn append(&mut self, epoch: u64, msg: IpcMessage) {
        match serde_json::to_vec(&msg) {
            Ok(frame) => self.store.put(epoch, &frame),
            Err(error) => eprintln!("[durable-outbox] serialize epoch {epoch}: {error}"),
        }
    }

    fn ack_through(&mut self, epoch: u64) {
        let target = epoch.max(self.acked_through).max(self.store.load_cursor());
        if target > self.acked_through {
            self.store.save_cursor(target);
            self.acked_through = target;
        }
        self.store.delete_through(target);
    }

    fn replay_from(&self, cursor: u64) -> Vec<(u64, IpcMessage)> {
        self.store
            .scan_after(cursor.max(self.acked_through).max(self.store.load_cursor()))
            .into_iter()
            .filter_map(|(epoch, frame)| match serde_json::from_slice(&frame) {
                Ok(message) => Some((epoch, message)),
                Err(error) => {
                    eprintln!("[durable-outbox] deserialize epoch {epoch}: {error}");
                    None
                }
            })
            .collect()
    }

    fn retained_epochs(&self) -> Vec<u64> {
        self.store
            .scan_after(self.acked_through.max(self.store.load_cursor()))
            .into_iter()
            .map(|(epoch, _)| epoch)
            .collect()
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryStore {
    entries: BTreeMap<u64, Vec<u8>>,
    cursor: u64,
}

impl OutboxStore for InMemoryStore {
    fn put(&mut self, epoch: u64, frame: &[u8]) {
        self.entries.insert(epoch, frame.to_vec());
    }

    fn delete_through(&mut self, epoch: u64) {
        self.entries.retain(|stored_epoch, _| *stored_epoch > epoch);
    }

    fn scan_after(&self, cursor: u64) -> Vec<(u64, Vec<u8>)> {
        self.entries
            .range((cursor.saturating_add(1))..)
            .map(|(epoch, frame)| (*epoch, frame.clone()))
            .collect()
    }

    fn load_cursor(&self) -> u64 {
        self.cursor
    }

    fn save_cursor(&mut self, epoch: u64) {
        self.cursor = self.cursor.max(epoch);
    }
}

pub type InMemoryOutbox = Outbox<InMemoryStore>;

impl Outbox<InMemoryStore> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for Outbox<InMemoryStore> {
    fn default() -> Self {
        Self::with_store(InMemoryStore::default())
    }
}

#[cfg(feature = "durable-sqlite")]
mod sqlite {
    use std::fmt;
    use std::path::Path;

    use rusqlite::{Connection, params, types::ValueRef};

    use super::{Outbox, OutboxStore};

    #[derive(Debug)]
    pub enum SqliteStoreError {
        Io(std::io::Error),
        Sqlite(rusqlite::Error),
    }

    impl fmt::Display for SqliteStoreError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Io(error) => write!(formatter, "SQLite outbox filesystem error: {error}"),
                Self::Sqlite(error) => write!(formatter, "SQLite outbox database error: {error}"),
            }
        }
    }

    impl std::error::Error for SqliteStoreError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Self::Io(error) => Some(error),
                Self::Sqlite(error) => Some(error),
            }
        }
    }

    impl From<std::io::Error> for SqliteStoreError {
        fn from(error: std::io::Error) -> Self {
            Self::Io(error)
        }
    }

    impl From<rusqlite::Error> for SqliteStoreError {
        fn from(error: rusqlite::Error) -> Self {
            Self::Sqlite(error)
        }
    }

    pub const OUTBOX_SCHEMA: &str = r#"
        CREATE TABLE IF NOT EXISTS reliable_sync_outbox (
            document_hash TEXT NOT NULL,
            epoch INTEGER NOT NULL,
            frame_json BLOB NOT NULL,
            PRIMARY KEY (document_hash, epoch)
        );
        CREATE TABLE IF NOT EXISTS reliable_sync_outbox_cursor (
            document_hash TEXT PRIMARY KEY,
            acked_through INTEGER NOT NULL DEFAULT 0
        );
    "#;

    pub fn ensure_outbox_schema(connection: &Connection) -> rusqlite::Result<()> {
        connection.execute_batch(OUTBOX_SCHEMA)
    }

    pub struct SqliteStore {
        connection: Connection,
        document_hash: String,
    }

    impl SqliteStore {
        pub fn open(
            path: &Path,
            document_hash: impl Into<String>,
        ) -> Result<Self, SqliteStoreError> {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            Self::with_connection(Connection::open(path)?, document_hash)
        }

        pub fn with_connection(
            connection: Connection,
            document_hash: impl Into<String>,
        ) -> Result<Self, SqliteStoreError> {
            ensure_outbox_schema(&connection)?;
            Ok(Self {
                connection,
                document_hash: document_hash.into(),
            })
        }

        pub fn document_hash(&self) -> &str {
            &self.document_hash
        }
    }

    impl OutboxStore for SqliteStore {
        fn put(&mut self, epoch: u64, frame: &[u8]) {
            if let Err(error) = self.connection.execute(
                "INSERT OR REPLACE INTO reliable_sync_outbox (document_hash, epoch, frame_json) VALUES (?1, ?2, ?3)",
                params![self.document_hash, epoch as i64, frame],
            ) {
                eprintln!("[durable-outbox] {}: append epoch {epoch}: {error}", self.document_hash);
            }
        }

        fn delete_through(&mut self, epoch: u64) {
            if let Err(error) = self.connection.execute(
                "DELETE FROM reliable_sync_outbox WHERE document_hash = ?1 AND epoch <= ?2",
                params![self.document_hash, epoch as i64],
            ) {
                eprintln!(
                    "[durable-outbox] {}: prune through {epoch}: {error}",
                    self.document_hash
                );
            }
        }

        fn scan_after(&self, cursor: u64) -> Vec<(u64, Vec<u8>)> {
            let mut statement = match self.connection.prepare(
                "SELECT epoch, frame_json FROM reliable_sync_outbox WHERE document_hash = ?1 AND epoch > ?2 ORDER BY epoch ASC",
            ) {
                Ok(statement) => statement,
                Err(error) => {
                    eprintln!("[durable-outbox] {}: prepare scan: {error}", self.document_hash);
                    return Vec::new();
                }
            };
            let rows =
                match statement.query_map(params![self.document_hash, cursor as i64], |row| {
                    let epoch = row.get::<_, i64>(0)?.max(0) as u64;
                    let value = row.get_ref(1)?;
                    let frame = match value {
                        ValueRef::Blob(bytes) | ValueRef::Text(bytes) => bytes.to_vec(),
                        // A `frame_json` cell that is NULL / INTEGER / REAL is a
                        // corrupt row, not an empty frame. Substituting `Vec::new()`
                        // (the former `_ => Vec::new()`) replayed a zero-byte frame
                        // to the peer as though it were the durable record for that
                        // epoch, and advanced the replay past it. Fail the row into
                        // the per-row error branch below: it is reported and skipped,
                        // and no fabricated frame reaches the wire.
                        other => {
                            return Err(rusqlite::Error::InvalidColumnType(
                                1,
                                format!("frame_json (epoch {epoch}) is {:?}", other.data_type()),
                                other.data_type(),
                            ));
                        }
                    };
                    Ok((epoch, frame))
                }) {
                    Ok(rows) => rows,
                    Err(error) => {
                        eprintln!("[durable-outbox] {}: scan: {error}", self.document_hash);
                        return Vec::new();
                    }
                };
            rows.filter_map(|row| match row {
                Ok(row) => Some(row),
                Err(error) => {
                    eprintln!("[durable-outbox] {}: read row: {error}", self.document_hash);
                    None
                }
            })
            .collect()
        }

        fn load_cursor(&self) -> u64 {
            self.connection.query_row(
                "SELECT acked_through FROM reliable_sync_outbox_cursor WHERE document_hash = ?1",
                params![self.document_hash],
                |row| row.get::<_, i64>(0),
            ).map(|cursor| cursor.max(0) as u64).unwrap_or(0)
        }

        fn save_cursor(&mut self, epoch: u64) {
            if let Err(error) = self.connection.execute(
                "INSERT INTO reliable_sync_outbox_cursor (document_hash, acked_through) VALUES (?1, ?2) \
                 ON CONFLICT(document_hash) DO UPDATE SET acked_through = \
                 MAX(reliable_sync_outbox_cursor.acked_through, excluded.acked_through)",
                params![self.document_hash, epoch as i64],
            ) {
                eprintln!("[durable-outbox] {}: save cursor {epoch}: {error}", self.document_hash);
            }
        }
    }

    pub type SqliteOutbox = Outbox<SqliteStore>;

    impl Outbox<SqliteStore> {
        pub fn open(
            path: &Path,
            document_hash: impl Into<String>,
        ) -> Result<Self, SqliteStoreError> {
            Ok(Self::with_store(SqliteStore::open(path, document_hash)?))
        }

        pub fn with_connection(
            connection: Connection,
            document_hash: impl Into<String>,
        ) -> Result<Self, SqliteStoreError> {
            Ok(Self::with_store(SqliteStore::with_connection(
                connection,
                document_hash,
            )?))
        }

        pub fn document_hash(&self) -> &str {
            self.store().document_hash()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::outbox::DurableOutbox;
        use crate::{Delta, IpcMessage};

        fn frame(epoch: u64) -> IpcMessage {
            IpcMessage::Delta(Delta::new(epoch.saturating_sub(1), epoch, vec![]))
        }

        /// `#failclosedsweep`: a `frame_json` cell of the wrong SQLite storage
        /// class is a corrupt row. It used to scan as `(epoch, vec![])` — an
        /// epoch the outbox reported as retained and replayable while carrying a
        /// zero-byte frame.
        #[test]
        fn a_corrupt_frame_column_is_reported_and_skipped_not_read_as_an_empty_frame() {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("corrupt.db");
            {
                let mut outbox = SqliteOutbox::open(&path, "doc").unwrap();
                outbox.append(1, frame(1));
                outbox.append(2, frame(2));
            }
            // Corrupt epoch 1's frame into an INTEGER column.
            let connection = Connection::open(&path).unwrap();
            connection
                .execute(
                    "UPDATE reliable_sync_outbox SET frame_json = 42 WHERE document_hash = ?1 AND epoch = 1",
                    params!["doc"],
                )
                .unwrap();
            drop(connection);

            let store = SqliteStore::open(&path, "doc").unwrap();
            let scanned = store.scan_after(0);
            assert_eq!(
                scanned.iter().map(|(e, _)| *e).collect::<Vec<_>>(),
                vec![2],
                "the corrupt row is skipped, not scanned as an empty frame"
            );
            assert!(
                !scanned.iter().any(|(_, frame)| frame.is_empty()),
                "no fabricated zero-byte frame reaches the caller"
            );

            let outbox = SqliteOutbox::open(&path, "doc").unwrap();
            assert_eq!(
                outbox.retained_epochs(),
                vec![2],
                "a corrupt row is not advertised as a retained, replayable epoch"
            );
            assert_eq!(
                outbox
                    .replay_from(0)
                    .into_iter()
                    .map(|(e, _)| e)
                    .collect::<Vec<_>>(),
                vec![2],
                "the well-formed row still replays"
            );
        }

        #[test]
        fn well_formed_blob_and_text_frames_still_scan() {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("mixed.db");
            {
                let mut outbox = SqliteOutbox::open(&path, "doc").unwrap();
                outbox.append(1, frame(1));
            }
            // A TEXT-typed frame column is the legacy `frame_json` shape and must
            // keep scanning.
            let connection = Connection::open(&path).unwrap();
            let text = serde_json::to_string(&frame(2)).unwrap();
            connection
                .execute(
                    "INSERT INTO reliable_sync_outbox (document_hash, epoch, frame_json) VALUES (?1, ?2, ?3)",
                    params!["doc", 2i64, text],
                )
                .unwrap();
            drop(connection);

            let store = SqliteStore::open(&path, "doc").unwrap();
            assert_eq!(
                store
                    .scan_after(0)
                    .iter()
                    .map(|(e, _)| *e)
                    .collect::<Vec<_>>(),
                vec![1, 2],
                "BLOB and TEXT frames both scan"
            );
        }
    }
}

#[cfg(feature = "durable-sqlite")]
pub use sqlite::{SqliteOutbox, SqliteStore, SqliteStoreError, ensure_outbox_schema};
