#![forbid(unsafe_code)]
#![allow(clippy::cast_possible_truncation)]

//! Physical filesystem storage device (`FsDevice`) backed by a dedicated I/O thread.
//!
//! Enforces ANVIL's performance doctrine:
//! - Never block consensus or actor threads on synchronous disk operations
//! - Uses `std::fs` and `rustix` (`fallocate`, `fdatasync`, `fsync`) without any unsafe code
//! - Processes operations sequentially on a dedicated I/O worker thread

use anvil_storage::{CompletionPayload, Device, DeviceCompletion, DeviceOp, FileId, StorageError};
use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;

/// Maps an abstract `FileId` to its physical relative path within `base_dir`.
#[must_use]
pub fn file_id_to_path(base_dir: &Path, file_id: FileId) -> PathBuf {
    if file_id.is_hard_state() {
        base_dir.join("hardstate.bin")
    } else if let Some(seg_id) = file_id.wal_segment_id() {
        base_dir.join(format!("wal-{seg_id:016x}.wal"))
    } else if file_id.is_snapshot() {
        let snap_id = file_id.0 & !(1u64 << 63);
        base_dir.join(format!("snapshot-{snap_id:016x}.snap"))
    } else {
        base_dir.join(format!("file-{:016x}.dat", file_id.0))
    }
}

/// Request dispatched across the thread boundary to the dedicated I/O thread.
enum IoRequest {
    Op(DeviceOp),
    Shutdown,
}

/// Real-file `Device` implementation backed by a dedicated I/O thread.
pub struct FsDevice {
    base_dir: PathBuf,
    op_sender: Option<Sender<IoRequest>>,
    completion_receiver: Receiver<DeviceCompletion>,
    io_thread_handle: Option<JoinHandle<()>>,
}

impl FsDevice {
    /// Spawns a new `FsDevice` operating in `base_dir`.
    ///
    /// The directory will be created if it does not already exist.
    ///
    /// # Errors
    /// Returns a `StorageError` if `base_dir` cannot be created.
    pub fn open(base_dir: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let base_dir = base_dir.into();
        std::fs::create_dir_all(&base_dir).map_err(|e| {
            StorageError::DeviceError(format!(
                "failed to create base directory {}: {e}",
                base_dir.display()
            ))
        })?;

        let (op_tx, op_rx) = crossbeam_channel::unbounded();
        let (comp_tx, comp_rx) = crossbeam_channel::unbounded();

        let worker_dir = base_dir.clone();
        let io_thread_handle = std::thread::Builder::new()
            .name("anvil-io-worker".to_string())
            .spawn(move || {
                io_worker_loop(&worker_dir, &op_rx, &comp_tx);
            })
            .map_err(|e| {
                StorageError::DeviceError(format!("failed to spawn I/O worker thread: {e}"))
            })?;

        Ok(Self {
            base_dir,
            op_sender: Some(op_tx),
            completion_receiver: comp_rx,
            io_thread_handle: Some(io_thread_handle),
        })
    }

    /// Returns a reference to the root storage directory.
    #[must_use]
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Blocks until the next completion arrives from the I/O thread, if any.
    ///
    /// Useful for synchronous testing and verification.
    #[must_use]
    pub fn wait_for_completion(&mut self) -> Option<DeviceCompletion> {
        self.completion_receiver.recv().ok()
    }

    /// Drains all currently queued completed operations without blocking.
    pub fn drain_ready_completions(&mut self) -> Vec<DeviceCompletion> {
        let mut ready = Vec::new();
        while let Ok(comp) = self.completion_receiver.try_recv() {
            ready.push(comp);
        }
        ready
    }
}

impl Device for FsDevice {
    fn submit(&mut self, op: DeviceOp) {
        if let Some(ref sender) = self.op_sender {
            let _ = sender.send(IoRequest::Op(op));
        }
    }

    fn poll_completion(&mut self) -> Option<DeviceCompletion> {
        self.completion_receiver.try_recv().ok()
    }
}

impl Drop for FsDevice {
    fn drop(&mut self) {
        if let Some(sender) = self.op_sender.take() {
            let _ = sender.send(IoRequest::Shutdown);
        }
        if let Some(handle) = self.io_thread_handle.take() {
            let _ = handle.join();
        }
    }
}

/// Worker loop executing on the dedicated I/O background thread.
fn io_worker_loop(
    base_dir: &Path,
    request_rx: &Receiver<IoRequest>,
    completion_tx: &Sender<DeviceCompletion>,
) {
    let mut open_files: BTreeMap<FileId, File> = BTreeMap::new();

    while let Ok(req) = request_rx.recv() {
        match req {
            IoRequest::Op(op) => {
                let op_id = op.op_id();
                let result = execute_fs_op(base_dir, &mut open_files, op);
                let _ = completion_tx.send(DeviceCompletion { op_id, result });
            }
            IoRequest::Shutdown => break,
        }
    }
}

/// Executes a single `DeviceOp` against the physical filesystem.
#[allow(clippy::too_many_lines)]
fn execute_fs_op(
    base_dir: &Path,
    open_files: &mut BTreeMap<FileId, File>,
    op: DeviceOp,
) -> Result<CompletionPayload, StorageError> {
    match op {
        DeviceOp::Create {
            file_id,
            preallocate_bytes,
            ..
        } => {
            let path = file_id_to_path(base_dir, file_id);
            if path.exists() {
                return Err(StorageError::DeviceError(format!(
                    "File already exists: {}",
                    path.display()
                )));
            }

            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(|e| {
                    StorageError::DeviceError(format!(
                        "failed to create file {}: {e}",
                        path.display()
                    ))
                })?;

            if preallocate_bytes > 0 {
                // Preallocate via rustix fallocate, falling back to set_len if unsupported
                match rustix::fs::fallocate(
                    &file,
                    rustix::fs::FallocateFlags::empty(),
                    0,
                    preallocate_bytes,
                ) {
                    Ok(()) => {}
                    Err(err)
                        if err == rustix::io::Errno::NOSYS
                            || err == rustix::io::Errno::OPNOTSUPP =>
                    {
                        file.set_len(preallocate_bytes).map_err(|e| {
                            StorageError::DeviceError(format!("set_len fallback failed: {e}"))
                        })?;
                    }
                    Err(err) => {
                        return Err(StorageError::DeviceError(format!(
                            "fallocate failed on {}: {err}",
                            path.display()
                        )));
                    }
                }
            }

            open_files.insert(file_id, file);
            Ok(CompletionPayload::Created)
        }

        DeviceOp::Open { file_id, .. } => {
            let _ = get_or_open_file(base_dir, open_files, file_id)?;
            Ok(CompletionPayload::Opened)
        }

        DeviceOp::Write {
            file_id,
            offset,
            data,
            ..
        } => {
            let file = get_or_open_file(base_dir, open_files, file_id)?;
            file.write_all_at(&data, offset).map_err(|e| {
                StorageError::DeviceError(format!("write failed at offset {offset}: {e}"))
            })?;
            Ok(CompletionPayload::Written(data.len()))
        }

        DeviceOp::Read {
            file_id,
            offset,
            len,
            ..
        } => {
            let file = get_or_open_file(base_dir, open_files, file_id)?;
            let file_len = file
                .metadata()
                .map_err(|e| StorageError::DeviceError(format!("metadata query failed: {e}")))?
                .len();

            if offset >= file_len {
                return Ok(CompletionPayload::Read(Bytes::new()));
            }

            let available = (file_len - offset).min(len as u64) as usize;
            let mut buf = vec![0u8; available];
            file.read_exact_at(&mut buf, offset).map_err(|e| {
                StorageError::DeviceError(format!("read_exact_at failed at offset {offset}: {e}"))
            })?;

            Ok(CompletionPayload::Read(Bytes::from(buf)))
        }

        DeviceOp::Fdatasync { file_id, .. } => {
            let file = get_or_open_file(base_dir, open_files, file_id)?;
            rustix::fs::fdatasync(file).map_err(|e| StorageError::FsyncFailed {
                reason: format!("fdatasync failed: {e}"),
            })?;
            Ok(CompletionPayload::Synced)
        }

        DeviceOp::SyncDir { .. } => {
            let dir_file = File::open(base_dir).map_err(|e| StorageError::FsyncFailed {
                reason: format!("failed to open directory for sync: {e}"),
            })?;
            rustix::fs::fsync(&dir_file).map_err(|e| StorageError::FsyncFailed {
                reason: format!("directory fsync failed: {e}"),
            })?;
            Ok(CompletionPayload::DirSynced)
        }

        DeviceOp::Rename { from, to, .. } => {
            open_files.remove(&from);
            open_files.remove(&to);

            let from_path = file_id_to_path(base_dir, from);
            let to_path = file_id_to_path(base_dir, to);

            std::fs::rename(&from_path, &to_path).map_err(|e| {
                StorageError::DeviceError(format!(
                    "rename failed from {} to {}: {e}",
                    from_path.display(),
                    to_path.display()
                ))
            })?;
            Ok(CompletionPayload::Renamed)
        }

        DeviceOp::Unlink { file_id, .. } => {
            open_files.remove(&file_id);
            let path = file_id_to_path(base_dir, file_id);
            if path.exists() {
                std::fs::remove_file(&path).map_err(|e| {
                    StorageError::DeviceError(format!("unlink failed for {}: {e}", path.display()))
                })?;
            }
            Ok(CompletionPayload::Unlinked)
        }

        DeviceOp::Truncate { file_id, len, .. } => {
            let file = get_or_open_file(base_dir, open_files, file_id)?;
            file.set_len(len).map_err(|e| {
                StorageError::DeviceError(format!("set_len failed to truncate to {len}: {e}"))
            })?;
            Ok(CompletionPayload::Truncated)
        }
    }
}

/// Retrieves an already open file descriptor or opens it from the filesystem.
fn get_or_open_file<'a>(
    base_dir: &Path,
    open_files: &'a mut BTreeMap<FileId, File>,
    file_id: FileId,
) -> Result<&'a mut File, StorageError> {
    use std::collections::btree_map::Entry;
    match open_files.entry(file_id) {
        Entry::Occupied(e) => Ok(e.into_mut()),
        Entry::Vacant(e) => {
            let path = file_id_to_path(base_dir, file_id);
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|e| {
                    StorageError::DeviceError(format!(
                        "failed to open file {}: {e}",
                        path.display()
                    ))
                })?;
            Ok(e.insert(file))
        }
    }
}
