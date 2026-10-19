// SPDX-License-Identifier: BUSL-1.1

//! Cross-thread core wake signaling for the Data Plane.
//!
//! When the Control Plane pushes a request into the SPSC ring buffer, it
//! signals the Data Plane core to wake from `libc::poll`. This replaces the
//! 50µs busy-poll sleep with an interrupt-driven wake.
//!
//! ## Descriptor ownership
//! The `EventFd` and every `EventFdNotifier` share ownership of the wake
//! descriptors. The descriptors close when the last holder drops. A notify
//! after the core thread exits writes into the still-open wake channel,
//! which nothing reads. The kernel cannot hand the descriptor number to
//! another file while any notifier lives.
//!
//! ## Platform backends
//! - **Linux**: `eventfd` in `EFD_SEMAPHORE` mode — the production path.
//! - **Other Unix (macOS, BSD)**: a self-pipe — slower per-wake than eventfd
//!   but functionally identical, so the server boots and serves on developer
//!   machines. This is a dev/correctness path, not a performance path.
//! - **Non-Unix (wasm, Windows)**: unsupported; `EventFd::new()` errors.

use std::sync::Arc;

#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

/// The descriptors behind one core wake channel.
///
/// Shared through `Arc` by the `EventFd` and its notifiers. `OwnedFd` closes
/// each descriptor exactly once, when the last `Arc` drops.
struct WakeFds {
    #[cfg(target_os = "linux")]
    fd: OwnedFd,
    // Self-pipe: the core reads `read_fd`, notifiers write `write_fd`.
    // Both ends stay open together, so a write never hits a reader-less pipe.
    #[cfg(all(unix, not(target_os = "linux")))]
    read_fd: OwnedFd,
    #[cfg(all(unix, not(target_os = "linux")))]
    write_fd: OwnedFd,
}

#[cfg(unix)]
impl WakeFds {
    /// The descriptor the core polls and drains.
    fn poll_fd(&self) -> RawFd {
        #[cfg(target_os = "linux")]
        {
            self.fd.as_raw_fd()
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.read_fd.as_raw_fd()
        }
    }

    /// The descriptor a notifier writes to.
    fn wake_fd(&self) -> RawFd {
        #[cfg(target_os = "linux")]
        {
            self.fd.as_raw_fd()
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.write_fd.as_raw_fd()
        }
    }
}

/// The Data Plane side of a core wake channel.
///
/// Each core owns one `EventFd`, moved into its thread at spawn. The Control
/// Plane holds `EventFdNotifier` clones, which are `Send + Sync`.
pub struct EventFd {
    fds: Arc<WakeFds>,
}

impl EventFd {
    /// Create a new core-wake handle.
    pub fn new() -> crate::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            // SAFETY: eventfd2 is a standard Linux syscall. EFD_SEMAPHORE makes
            // each read decrement by 1, EFD_NONBLOCK prevents blocking reads,
            // EFD_CLOEXEC keeps the descriptor out of exec'd children.
            let raw = unsafe {
                libc::eventfd(
                    0,
                    libc::EFD_NONBLOCK | libc::EFD_SEMAPHORE | libc::EFD_CLOEXEC,
                )
            };
            if raw < 0 {
                return Err(crate::Error::Io(std::io::Error::last_os_error()));
            }
            // SAFETY: eventfd returned a fresh descriptor that nothing else owns.
            let fd = unsafe { OwnedFd::from_raw_fd(raw) };
            Ok(Self {
                fds: Arc::new(WakeFds { fd }),
            })
        }
        #[cfg(all(unix, not(target_os = "linux")))]
        {
            let mut raw = [0 as libc::c_int; 2];
            // SAFETY: `raw` points to two valid c_int slots for pipe() to fill.
            if unsafe { libc::pipe(raw.as_mut_ptr()) } < 0 {
                return Err(crate::Error::Io(std::io::Error::last_os_error()));
            }
            // SAFETY: pipe() returned two fresh descriptors that nothing else owns.
            let (read_fd, write_fd) =
                unsafe { (OwnedFd::from_raw_fd(raw[0]), OwnedFd::from_raw_fd(raw[1])) };
            // Both ends non-blocking + close-on-exec: drain() must not block on
            // an empty pipe, and notify() must not block on a full one.
            // On error both `OwnedFd`s drop and close their descriptors.
            set_pipe_flags(read_fd.as_raw_fd())
                .and_then(|()| set_pipe_flags(write_fd.as_raw_fd()))
                .map_err(crate::Error::Io)?;
            Ok(Self {
                fds: Arc::new(WakeFds { read_fd, write_fd }),
            })
        }
        #[cfg(not(unix))]
        {
            Err(crate::Error::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "data-plane core wake is only available on Unix platforms",
            )))
        }
    }

    /// Drain all pending notifications (non-blocking).
    ///
    /// Returns the number of signals accumulated since the last drain.
    /// Returns 0 if no signals are pending.
    pub fn drain(&self) -> u64 {
        #[cfg(target_os = "linux")]
        {
            let mut buf = 0u64;
            // SAFETY: reading 8 bytes from an eventfd is the documented API.
            // `self.fds` keeps the descriptor open for the whole call.
            let ret = unsafe {
                libc::read(
                    self.fds.poll_fd(),
                    &mut buf as *mut u64 as *mut libc::c_void,
                    std::mem::size_of::<u64>(),
                )
            };
            if ret == 8 { buf } else { 0 }
        }
        #[cfg(all(unix, not(target_os = "linux")))]
        {
            // Read every queued wake byte until the pipe drains (EAGAIN). The
            // caller's `while drain() > 0 {}` loop relies on a 0 once empty.
            let mut total: u64 = 0;
            let mut buf = [0u8; 256];
            loop {
                // SAFETY: reading into a valid local buffer from a non-blocking
                // fd that `self.fds` keeps open for the whole call.
                let ret = unsafe {
                    libc::read(
                        self.fds.poll_fd(),
                        buf.as_mut_ptr() as *mut libc::c_void,
                        buf.len(),
                    )
                };
                if ret > 0 {
                    total = total.saturating_add(ret as u64);
                    // A short read means the pipe is now empty.
                    if (ret as usize) < buf.len() {
                        return total;
                    }
                    continue;
                }
                // ret == 0 (EOF) or ret < 0 (EAGAIN / error): nothing more to read.
                return total;
            }
        }
        #[cfg(not(unix))]
        {
            0
        }
    }

    /// Block until a signal arrives, with a timeout.
    ///
    /// Returns `true` if a signal was received, `false` on timeout.
    pub fn poll_wait(&self, timeout_ms: i32) -> bool {
        #[cfg(unix)]
        {
            let mut pfd = libc::pollfd {
                fd: self.fds.poll_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: standard poll syscall on a fd that `self.fds` keeps open.
            let ret = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
            ret > 0 && (pfd.revents & libc::POLLIN) != 0
        }
        #[cfg(not(unix))]
        {
            let _ = timeout_ms;
            false
        }
    }

    /// Create a `Send + Sync` notifier handle for the Control Plane.
    ///
    /// The notifier shares ownership of the wake descriptors. They stay open
    /// until this `EventFd` and every notifier drop.
    pub fn notifier(&self) -> EventFdNotifier {
        EventFdNotifier {
            fds: Arc::clone(&self.fds),
        }
    }
}

/// Set `O_NONBLOCK` and `FD_CLOEXEC` on a freshly-created pipe fd.
#[cfg(all(unix, not(target_os = "linux")))]
fn set_pipe_flags(fd: RawFd) -> std::io::Result<()> {
    // SAFETY: fcntl is called with a valid fd returned by pipe().
    let status_flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if status_flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: F_SETFL updates the fd's status flags.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, status_flags | libc::O_NONBLOCK) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: fcntl is called with a valid fd returned by pipe().
    let fd_flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if fd_flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: F_SETFD updates the fd's close-on-exec flag.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, fd_flags | libc::FD_CLOEXEC) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// A `Send + Sync` handle for signaling an `EventFd` from the Control Plane.
///
/// Every clone shares ownership of the wake descriptors with the `EventFd`.
/// A notify after the core thread exits lands in the still-open wake channel.
/// It never reaches a file that reused the descriptor number.
#[derive(Clone)]
pub struct EventFdNotifier {
    fds: Arc<WakeFds>,
}

impl EventFdNotifier {
    /// Signal the Data Plane core to wake up.
    pub fn notify(&self) {
        #[cfg(target_os = "linux")]
        {
            let val: u64 = 1;
            // SAFETY: writing 8 bytes to an eventfd is the documented API.
            // The write is atomic and thread-safe per the Linux man page.
            // `self.fds` keeps the descriptor open for the whole call.
            // A full counter returns EAGAIN, which leaves a wake pending.
            unsafe {
                libc::write(
                    self.fds.wake_fd(),
                    &val as *const u64 as *const libc::c_void,
                    std::mem::size_of::<u64>(),
                );
            }
        }
        #[cfg(all(unix, not(target_os = "linux")))]
        {
            let val: u8 = 1;
            // SAFETY: writing 1 byte to the self-pipe write end, which
            // `self.fds` keeps open for the whole call. A full pipe (EAGAIN)
            // already means a wake is pending, so the level-triggered poll
            // still fires.
            unsafe {
                libc::write(
                    self.fds.wake_fd(),
                    &val as *const u8 as *const libc::c_void,
                    1,
                );
            }
        }
        #[cfg(not(unix))]
        {
            let _ = &self.fds;
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    /// Whether `fd` names an open descriptor in this process.
    fn is_open(fd: RawFd) -> bool {
        // SAFETY: F_GETFD only reads descriptor flags. An unused number fails
        // with EBADF and touches nothing.
        unsafe { libc::fcntl(fd, libc::F_GETFD) >= 0 }
    }

    #[test]
    fn create_and_signal() {
        let efd = EventFd::new().unwrap();
        let notifier = efd.notifier();

        // No signals yet.
        assert_eq!(efd.drain(), 0);

        // Signal and drain — one notify yields one pending signal on both
        // backends (eventfd semaphore read == 1, self-pipe byte == 1).
        notifier.notify();
        assert_eq!(efd.drain(), 1);

        // Drained — should be 0 again.
        assert_eq!(efd.drain(), 0);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn multiple_signals_accumulate() {
        let efd = EventFd::new().unwrap();
        let notifier = efd.notifier();

        notifier.notify();
        notifier.notify();
        notifier.notify();

        // EFD_SEMAPHORE mode: each read returns 1, decrements by 1.
        assert_eq!(efd.drain(), 1);
        assert_eq!(efd.drain(), 1);
        assert_eq!(efd.drain(), 1);
        assert_eq!(efd.drain(), 0);
    }

    #[test]
    #[cfg(all(unix, not(target_os = "linux")))]
    fn multiple_signals_coalesce() {
        let efd = EventFd::new().unwrap();
        let notifier = efd.notifier();

        notifier.notify();
        notifier.notify();
        notifier.notify();

        // Self-pipe: a single drain consumes all queued wake bytes, then 0.
        assert_eq!(efd.drain(), 3);
        assert_eq!(efd.drain(), 0);
    }

    #[test]
    fn poll_wait_timeout() {
        let efd = EventFd::new().unwrap();
        // No signal — should timeout quickly.
        assert!(!efd.poll_wait(1));
    }

    #[test]
    fn poll_wait_signaled() {
        let efd = EventFd::new().unwrap();
        let notifier = efd.notifier();

        notifier.notify();
        assert!(efd.poll_wait(100));
    }

    #[test]
    fn notifier_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EventFdNotifier>();
    }

    /// A notify after the core's `EventFd` drops must not touch a file opened
    /// afterwards. The notifier keeps the descriptor number out of reuse.
    #[test]
    fn notify_after_the_core_dropped_leaves_a_later_file_intact() {
        let efd = EventFd::new().unwrap();
        let wake_fd = efd.fds.wake_fd();
        let notifier = efd.notifier();
        drop(efd); // The core thread exits.

        // A file opened now takes the lowest free number.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let mut header = b"redb\x1a\n\xa9\r\n".to_vec();
        header.resize(4096, 0);
        std::fs::write(&path, &header).unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        assert_ne!(
            file.as_raw_fd(),
            wake_fd,
            "the kernel reused the wake descriptor number while a notifier lives"
        );

        notifier.notify(); // The Control Plane wakes a core that no longer exists.
        drop(file);

        let after = std::fs::read(&path).unwrap();
        assert_eq!(
            &after[..9],
            &header[..9],
            "redb header overwritten: {:02x?}",
            &after[..16]
        );
        assert_eq!(after, header, "file body modified by a notify");
    }

    /// The wake descriptors stay open while any holder lives and close when
    /// the last one drops.
    #[test]
    fn descriptor_closes_only_when_the_last_holder_drops() {
        let efd = EventFd::new().unwrap();
        let poll_fd = efd.fds.poll_fd();
        let wake_fd = efd.fds.wake_fd();
        let first = efd.notifier();
        let second = first.clone();

        drop(efd);
        assert!(
            is_open(poll_fd) && is_open(wake_fd),
            "closed with two notifiers alive"
        );

        drop(first);
        assert!(
            is_open(poll_fd) && is_open(wake_fd),
            "closed with one notifier alive"
        );
        second.notify();

        drop(second);
        assert!(
            !is_open(poll_fd),
            "poll descriptor leaked after the last holder dropped"
        );
        assert!(
            !is_open(wake_fd),
            "wake descriptor leaked after the last holder dropped"
        );
    }
}
