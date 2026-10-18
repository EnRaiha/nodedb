// SPDX-License-Identifier: BUSL-1.1

//! The server file a `COPY ... FROM '<path>'` reads, opened once.
//!
//! The open is non-blocking, so a FIFO or a device never stalls it. Every
//! check then runs on the open handle: the file type, the size cap, and
//! each read. No second path lookup can resolve to another file.

use tokio::io::AsyncReadExt;

use crate::control::server::shared::ddl::result::DdlError;

/// Maximum file size accepted for COPY FROM (16 GiB).
const MAX_FILE_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// An open regular file under the size cap.
pub(crate) struct CopySourceFile {
    file: tokio::fs::File,
    path: String,
    /// The size `fstat` reported at open.
    len: u64,
    /// Bytes `read_some` returned so far.
    consumed: u64,
}

impl CopySourceFile {
    /// Open `path` and check it through the handle. A missing or unreadable
    /// path and a non-regular file are `58030`. A file over the cap is
    /// `54000`.
    pub(crate) async fn open(path: &str) -> Result<Self, DdlError> {
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(path)
            .await
            .map_err(|e| DdlError::new("58030", format!("COPY: cannot open '{path}': {e}")))?;
        let metadata = file
            .metadata()
            .await
            .map_err(|e| DdlError::new("58030", format!("COPY: cannot stat '{path}': {e}")))?;
        if !metadata.is_file() {
            return Err(DdlError::new(
                "58030",
                format!(
                    "COPY: '{path}' is not a regular file; \
                     COPY reads regular files only, never a FIFO, device, or directory"
                ),
            ));
        }
        let len = metadata.len();
        if len > MAX_FILE_BYTES {
            return Err(too_large(path, len));
        }
        // A read of a regular file never blocks, so `O_NONBLOCK` changes
        // nothing past the open.
        Ok(Self {
            file,
            path: path.to_owned(),
            len,
            consumed: 0,
        })
    }

    /// The open handle as a blocking file, for a decoder that runs on a
    /// blocking thread. The handle is the one every open-time check ran on.
    pub(crate) async fn into_std(self) -> std::fs::File {
        self.file.into_std().await
    }

    /// Read the next bytes into `buf`, `0` at the end of the file. A file
    /// that grew past the cap after the open is `54000`.
    pub(crate) async fn read_some(&mut self, buf: &mut [u8]) -> Result<usize, DdlError> {
        let read = self.file.read(buf).await;
        let n = read.map_err(|e| self.read_error(e))?;
        self.consumed = self.consumed.saturating_add(n as u64);
        if self.consumed > MAX_FILE_BYTES {
            return Err(too_large(&self.path, self.consumed));
        }
        Ok(n)
    }

    /// Read the whole file through the handle. A file that grew past the
    /// cap after the open is `54000`.
    pub(crate) async fn read_all(&mut self) -> Result<Vec<u8>, DdlError> {
        let hint = usize::try_from(self.len).unwrap_or(0);
        let mut bytes = Vec::with_capacity(hint);
        let read = (&mut self.file)
            .take(MAX_FILE_BYTES + 1)
            .read_to_end(&mut bytes)
            .await;
        read.map_err(|e| self.read_error(e))?;
        let total = bytes.len() as u64;
        if total > MAX_FILE_BYTES {
            return Err(too_large(&self.path, total));
        }
        Ok(bytes)
    }

    /// The `58030` error of a failed read.
    fn read_error(&self, e: std::io::Error) -> DdlError {
        DdlError::new("58030", format!("COPY: cannot read '{}': {e}", self.path))
    }
}

fn too_large(path: &str, len: u64) -> DdlError {
    DdlError::new(
        "54000",
        format!("COPY: file '{path}' is {len} bytes, exceeds limit of {MAX_FILE_BYTES} bytes"),
    )
}
