// SPDX-License-Identifier: BUSL-1.1

//! Reads and writes of a `file://` backup object through a directory fd of
//! the canonical local root.
//!
//! Every component opens relative to its parent's fd, one at a time, and no
//! open follows a symlink: on Linux through `openat2` with
//! `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS`, elsewhere through `openat` with
//! `O_NOFOLLOW`. A missing directory is created with `mkdirat` relative to
//! its parent's fd. A component swapped for a symlink after the URI resolved
//! therefore refuses the read or write: there is no gap between a check and
//! the open.
//!
//! A listing names only the regular files of one directory, never a symlink.
//! A deletion unlinks a regular file relative to its parent's fd and refuses
//! a symlink, so no deletion reaches a file outside the root.

use std::ffi::{CStr, CString};
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd};
use std::path::Path;

/// Why a local read or write did not complete.
#[derive(Debug)]
pub(super) enum LocalIoError {
    /// A component is a symlink, or the path leaves the root.
    Escapes,
    Io(io::Error),
}

impl From<io::Error> for LocalIoError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

/// Open the canonical root `path` as a directory fd.
pub(super) fn open_root(path: &Path) -> io::Result<OwnedFd> {
    use std::os::unix::fs::OpenOptionsExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
        .open(path)?;
    Ok(OwnedFd::from(file))
}

/// Write `bytes` as `components` below `root`: every component but the last
/// is a directory, created when missing. The file is written under a
/// temporary name, synced, then renamed over the final name.
pub(super) fn write_beneath(
    root: &OwnedFd,
    components: &[String],
    bytes: &[u8],
) -> Result<(), LocalIoError> {
    let (name, dirs) = components.split_last().ok_or(LocalIoError::Escapes)?;
    let mut parent = root.try_clone()?;
    for dir in dirs {
        let dir = c_name(dir)?;
        // SAFETY: `parent` is a live directory fd and `dir` a NUL-terminated
        // single component.
        let made = unsafe { libc::mkdirat(parent.as_raw_fd(), dir.as_ptr(), 0o750) };
        if made != 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EEXIST) {
                return Err(error.into());
            }
        }
        parent = open_dir_at(parent.as_fd(), &dir)?;
    }
    let final_name = c_name(name)?;
    let partial = c_name(&format!(".{name}.partial"))?;
    let fd = open_at(
        parent.as_fd(),
        &partial,
        libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC | libc::O_CLOEXEC,
        0o640,
    )?;
    let mut file = File::from(fd);
    file.write_all(bytes)?;
    file.sync_all()?;
    // SAFETY: both names are NUL-terminated single components under the same
    // live directory fd. `renameat` replaces the final name itself, never a
    // symlink's target.
    let renamed = unsafe {
        libc::renameat(
            parent.as_raw_fd(),
            partial.as_ptr(),
            parent.as_raw_fd(),
            final_name.as_ptr(),
        )
    };
    if renamed != 0 {
        return Err(io::Error::last_os_error().into());
    }
    File::from(parent).sync_all()?;
    Ok(())
}

/// Read the whole file `components` below `root`.
pub(super) fn read_beneath(root: &OwnedFd, components: &[String]) -> Result<Vec<u8>, LocalIoError> {
    let (name, dirs) = components.split_last().ok_or(LocalIoError::Escapes)?;
    let mut parent = root.try_clone()?;
    for dir in dirs {
        parent = open_dir_at(parent.as_fd(), &c_name(dir)?)?;
    }
    let fd = open_at(
        parent.as_fd(),
        &c_name(name)?,
        libc::O_RDONLY | libc::O_CLOEXEC,
        0,
    )?;
    let mut bytes = Vec::new();
    File::from(fd).read_to_end(&mut bytes)?;
    Ok(bytes)
}

/// The names of the regular files directly in the directory `components`
/// below `root`, sorted. A missing directory holds none. A symlink, a
/// directory and a name that is not UTF-8 are never listed.
pub(super) fn list_files(
    root: &OwnedFd,
    components: &[String],
) -> Result<Vec<String>, LocalIoError> {
    let Some(dir) = open_dirs(root, components)? else {
        return Ok(Vec::new());
    };
    let mut files = Vec::new();
    for name in read_dir_names(dir.try_clone()?)? {
        let Ok(c) = CString::new(name.as_str()) else {
            continue;
        };
        if file_type_at(dir.as_fd(), &c)? == Some(libc::S_IFREG) {
            files.push(name);
        }
    }
    files.sort();
    Ok(files)
}

/// Delete the regular file `components` below `root`. A missing file counts
/// as deleted. A symlink is refused as [`LocalIoError::Escapes`]: the
/// deletion never reaches its target.
pub(super) fn delete_beneath(root: &OwnedFd, components: &[String]) -> Result<(), LocalIoError> {
    let (name, dirs) = components.split_last().ok_or(LocalIoError::Escapes)?;
    let Some(parent) = open_dirs(root, dirs)? else {
        return Ok(());
    };
    let name = c_name(name)?;
    match file_type_at(parent.as_fd(), &name)? {
        None => return Ok(()),
        Some(kind) if kind == libc::S_IFLNK => return Err(LocalIoError::Escapes),
        Some(_) => {}
    }
    // SAFETY: `parent` is a live directory fd and `name` a NUL-terminated
    // single component. `unlinkat` removes the entry itself and follows no
    // symlink.
    let unlinked = unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), 0) };
    if unlinked != 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ENOENT) {
            return Err(error.into());
        }
    }
    Ok(())
}

/// Open the directories `components` below `root`, one at a time. `None`
/// when one is missing.
fn open_dirs(root: &OwnedFd, components: &[String]) -> Result<Option<OwnedFd>, LocalIoError> {
    let mut dir = root.try_clone()?;
    for component in components {
        dir = match open_dir_at(dir.as_fd(), &c_name(component)?) {
            Ok(fd) => fd,
            Err(LocalIoError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
    }
    Ok(Some(dir))
}

/// Every entry name of the directory `dir`, except `.` and `..`.
fn read_dir_names(dir: OwnedFd) -> io::Result<Vec<String>> {
    let raw = dir.into_raw_fd();
    // SAFETY: `raw` is an open directory fd this call owns. `fdopendir` takes
    // it over, and `closedir` below closes it.
    let stream = unsafe { libc::fdopendir(raw) };
    if stream.is_null() {
        let error = io::Error::last_os_error();
        // SAFETY: `fdopendir` failed, so `raw` is still owned here.
        unsafe { libc::close(raw) };
        return Err(error);
    }
    let mut names = Vec::new();
    let outcome = loop {
        clear_errno();
        // SAFETY: `stream` is a live directory stream.
        let entry = unsafe { libc::readdir(stream) };
        if entry.is_null() {
            let error = io::Error::last_os_error();
            break match error.raw_os_error() {
                Some(0) | None => Ok(()),
                Some(_) => Err(error),
            };
        }
        // SAFETY: `readdir` returned a live entry whose `d_name` is
        // NUL-terminated, valid until the next `readdir`.
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        if let Ok(name) = name.to_str()
            && name != "."
            && name != ".."
        {
            names.push(name.to_owned());
        }
    };
    // SAFETY: `stream` is live and closed once.
    unsafe { libc::closedir(stream) };
    outcome.map(|()| names)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn clear_errno() {
    // SAFETY: the thread's errno slot is always valid to write.
    unsafe { *libc::__errno_location() = 0 };
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
fn clear_errno() {
    // SAFETY: the thread's errno slot is always valid to write.
    unsafe { *libc::__error() = 0 };
}

#[cfg(any(target_os = "netbsd", target_os = "openbsd"))]
fn clear_errno() {
    // SAFETY: the thread's errno slot is always valid to write.
    unsafe { *libc::__errno() = 0 };
}

fn c_name(name: &str) -> Result<CString, LocalIoError> {
    if name.is_empty() || name == "." || name == ".." || name.contains('/') {
        return Err(LocalIoError::Escapes);
    }
    CString::new(name).map_err(|_| LocalIoError::Escapes)
}

fn open_dir_at(parent: BorrowedFd<'_>, name: &CString) -> Result<OwnedFd, LocalIoError> {
    open_at(
        parent,
        name,
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        0,
    )
}

/// Open the single component `name` below `parent` without following a
/// symlink. A symlink, or a resolution that leaves `parent`, is
/// [`LocalIoError::Escapes`].
fn open_at(
    parent: BorrowedFd<'_>,
    name: &CString,
    flags: libc::c_int,
    mode: libc::mode_t,
) -> Result<OwnedFd, LocalIoError> {
    match open_no_symlinks(parent, name, flags, mode) {
        Ok(fd) => Ok(fd),
        Err(error) => Err(classify(parent, name, error)),
    }
}

#[cfg(target_os = "linux")]
fn open_no_symlinks(
    parent: BorrowedFd<'_>,
    name: &CString,
    flags: libc::c_int,
    mode: libc::mode_t,
) -> io::Result<OwnedFd> {
    // SAFETY: `open_how` is plain data; zero is a valid value of every field.
    let mut how: libc::open_how = unsafe { std::mem::zeroed() };
    how.flags = flags as u64;
    how.mode = u64::from(mode);
    how.resolve = libc::RESOLVE_BENEATH | libc::RESOLVE_NO_SYMLINKS;
    // SAFETY: `parent` is a live directory fd, `name` is NUL-terminated, and
    // `how` lives for the call with its exact size passed.
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            parent.as_raw_fd(),
            name.as_ptr(),
            &how as *const libc::open_how,
            std::mem::size_of::<libc::open_how>(),
        )
    };
    if fd >= 0 {
        // SAFETY: the kernel returned a new fd this call owns.
        return Ok(unsafe { OwnedFd::from_raw_fd(fd as libc::c_int) });
    }
    let error = io::Error::last_os_error();
    // A kernel without `openat2` still refuses a symlink through `O_NOFOLLOW`
    // on this single component.
    if error.raw_os_error() == Some(libc::ENOSYS) {
        return openat_nofollow(parent, name, flags, mode);
    }
    Err(error)
}

#[cfg(not(target_os = "linux"))]
fn open_no_symlinks(
    parent: BorrowedFd<'_>,
    name: &CString,
    flags: libc::c_int,
    mode: libc::mode_t,
) -> io::Result<OwnedFd> {
    openat_nofollow(parent, name, flags, mode)
}

fn openat_nofollow(
    parent: BorrowedFd<'_>,
    name: &CString,
    flags: libc::c_int,
    mode: libc::mode_t,
) -> io::Result<OwnedFd> {
    // SAFETY: `parent` is a live directory fd and `name` a NUL-terminated
    // single component.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_NOFOLLOW,
            libc::c_uint::from(mode),
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `openat` returned a new fd this call owns.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// A symlink or an escape is [`LocalIoError::Escapes`]. `ENOTDIR` is one only
/// when the component is a symlink: a regular file in a directory's place is
/// an I/O error.
fn classify(parent: BorrowedFd<'_>, name: &CString, error: io::Error) -> LocalIoError {
    match error.raw_os_error() {
        Some(libc::ELOOP) | Some(libc::EXDEV) => LocalIoError::Escapes,
        Some(libc::ENOTDIR) if is_symlink_at(parent, name) => LocalIoError::Escapes,
        _ => LocalIoError::Io(error),
    }
}

fn is_symlink_at(parent: BorrowedFd<'_>, name: &CString) -> bool {
    matches!(file_type_at(parent, name), Ok(Some(kind)) if kind == libc::S_IFLNK)
}

/// The file type bits of `name` below `parent`, without following a symlink.
/// `None` when it is missing.
fn file_type_at(parent: BorrowedFd<'_>, name: &CString) -> io::Result<Option<libc::mode_t>> {
    // SAFETY: `stat` is plain data, filled by the call.
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: `parent` is a live directory fd, `name` is NUL-terminated, and
    // `stat` outlives the call.
    let found = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            &mut stat,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if found != 0 {
        let error = io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ENOENT) => Ok(None),
            _ => Err(error),
        };
    }
    Ok(Some(stat.st_mode & libc::S_IFMT))
}
