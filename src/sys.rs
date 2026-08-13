//! Resident set size, reported by `lum status`.
//!
//! lum exists in its current shape because of a memory measurement, so making
//! that measurement a first-class thing the tool reports about itself — rather
//! than something you go to `ps` for — is the point. A regression here should
//! be visible to whoever notices it first.

/// Current resident bytes, or 0 if the platform will not say.
pub fn resident_bytes() -> u64 {
    imp::resident_bytes()
}

#[cfg(target_os = "linux")]
mod imp {
    pub fn resident_bytes() -> u64 {
        // statm's second field is resident pages. /proc/self/status has the
        // same number in kB but costs a much larger read to find it.
        let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else {
            return 0;
        };
        let Some(pages) = statm.split_whitespace().nth(1).and_then(|f| f.parse::<u64>().ok())
        else {
            return 0;
        };
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size <= 0 {
            return 0;
        }
        pages * page_size as u64
    }
}

#[cfg(target_os = "macos")]
mod imp {
    // Darwin has no /proc, so this asks the kernel directly. The shape is
    // fixed by the mach ABI: a struct, its size in 32-bit words, and an
    // out-parameter that is both.
    #[repr(C)]
    struct MachTaskBasicInfo {
        virtual_size: u64,
        resident_size: u64,
        resident_size_max: u64,
        user_time: [i32; 2],
        system_time: [i32; 2],
        policy: i32,
        suspend_count: i32,
    }

    const MACH_TASK_BASIC_INFO: i32 = 20;

    extern "C" {
        fn mach_task_self() -> u32;
        fn task_info(
            task: u32,
            flavor: i32,
            task_info_out: *mut i32,
            task_info_count: *mut u32,
        ) -> i32;
    }

    pub fn resident_bytes() -> u64 {
        let mut info = MachTaskBasicInfo {
            virtual_size: 0,
            resident_size: 0,
            resident_size_max: 0,
            user_time: [0; 2],
            system_time: [0; 2],
            policy: 0,
            suspend_count: 0,
        };
        let mut count =
            (std::mem::size_of::<MachTaskBasicInfo>() / std::mem::size_of::<i32>()) as u32;
        // Safety: `info` is a live, correctly sized MachTaskBasicInfo and
        // `count` describes it in the 32-bit words the ABI expects. A non-zero
        // return means nothing was written, which is why it is checked before
        // the field is read.
        let result = unsafe {
            task_info(
                mach_task_self(),
                MACH_TASK_BASIC_INFO,
                &mut info as *mut _ as *mut i32,
                &mut count,
            )
        };
        if result != 0 {
            return 0;
        }
        info.resident_size
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod imp {
    pub fn resident_bytes() -> u64 {
        0
    }
}

/// Human-readable bytes, for `status` and `top`.
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else if value >= 100.0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn this_process_reports_a_plausible_resident_size() {
        // Anything running a test harness is over a megabyte and under a
        // hundred gigabytes; the point is to catch a zero or a garbage read.
        let rss = resident_bytes();
        assert!(rss > 1 << 20, "implausibly small: {rss}");
        assert!(rss < 100 << 30, "implausibly large: {rss}");
    }

    #[test]
    fn byte_sizes_read_the_way_a_person_would_write_them() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1536), "1.5 KB");
        assert_eq!(human_bytes(45 << 20), "45.0 MB");
        assert_eq!(human_bytes(4_507 << 20), "4.4 GB");
    }
}
