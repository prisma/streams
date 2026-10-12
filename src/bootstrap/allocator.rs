//! The process's allocator posture.
//!
//! Transparent huge pages are off for the whole process. With them on, the
//! kernel's khugepaged collapses the 4 KiB holes mimalloc purges back into
//! 2 MiB pages, so memory freed after a load returns to RSS within minutes
//! (Q0: an idle instance climbed from 119-122 to 154-162 MiB within 90 s of
//! releasing its caches, against 79-81 MiB with huge pages off).

/// What the kernel reports after the process asked it to stop backing this
/// process's memory with transparent huge pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThpPosture {
    /// The kernel reads the flag back as set: no huge page backs this
    /// process's anonymous memory, at fault time or by collapse.
    Off,
    /// The kernel refused the request, with this errno.
    Refused(i32),
    /// The request succeeded but the flag read back as this value.
    Unconfirmed(i32),
    /// Not Linux: there are no transparent huge pages to turn off.
    NotLinux,
}

/// Turns transparent huge pages off for this process and its future
/// children, then reads the flag back. The binary calls this first, before
/// the allocator maps the arenas the service runs on. The flag holds under
/// both kernel modes (`always` and `madvise`), whatever the allocator asks
/// for, which an allocator option alone does not.
#[cfg(target_os = "linux")]
pub fn disable_transparent_huge_pages() -> ThpPosture {
    let (zero, one): (libc::c_ulong, libc::c_ulong) = (0, 1);
    // SAFETY: PR_SET_THP_DISABLE takes only integer arguments and changes
    // this process's huge-page eligibility; no memory is passed to the
    // kernel. The unused arguments are passed as zero, as it requires.
    if unsafe { libc::prctl(libc::PR_SET_THP_DISABLE, one, zero, zero, zero) } != 0 {
        let errno = std::io::Error::last_os_error().raw_os_error();
        return ThpPosture::Refused(errno.unwrap_or_default());
    }
    // SAFETY: as above; PR_GET_THP_DISABLE reads the flag back.
    match unsafe { libc::prctl(libc::PR_GET_THP_DISABLE, zero, zero, zero, zero) } {
        1 => ThpPosture::Off,
        other => ThpPosture::Unconfirmed(other),
    }
}

/// Not Linux: nothing to turn off.
#[cfg(not(target_os = "linux"))]
pub fn disable_transparent_huge_pages() -> ThpPosture {
    ThpPosture::NotLinux
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{ThpPosture, disable_transparent_huge_pages};

    #[test]
    fn the_process_posture_turns_transparent_huge_pages_off() {
        let posture = disable_transparent_huge_pages();
        let zero: libc::c_ulong = 0;
        // SAFETY: PR_GET_THP_DISABLE takes only integer arguments and reads
        // this process's flag; no memory is passed to the kernel.
        let flag = unsafe { libc::prctl(libc::PR_GET_THP_DISABLE, zero, zero, zero, zero) };
        assert_eq!(flag, 1);
        assert_eq!(posture, ThpPosture::Off);
    }
}
