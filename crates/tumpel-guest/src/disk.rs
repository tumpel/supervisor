use std::ffi::CString;
use std::mem::MaybeUninit;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

const MIB: u64 = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Space {
    pub total_mib: u64,
    pub available_mib: u64,
    pub free_mib: u64,
}

impl Space {
    #[must_use]
    pub fn full(&self) -> bool {
        self.free_mib < self.total_mib / 100
    }
}

pub fn space(path: &Path) -> Result<Space, String> {
    let named = CString::new(path.as_os_str().as_bytes())
        .map_err(|error| format!("{} holds a NUL byte: {error}", path.display()))?;
    let mut stat = MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(named.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(format!(
            "reading the disk space under {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        ));
    }
    let stat = unsafe { stat.assume_init() };
    let mib = |blocks: u64| blocks * stat.f_frsize / MIB;
    Ok(Space {
        total_mib: mib(stat.f_blocks),
        available_mib: mib(stat.f_bavail),
        free_mib: mib(stat.f_bfree),
    })
}
