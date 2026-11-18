use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU32;

pub const TAP_POOL: u8 = 64;

#[must_use]
pub fn headroom_mib(total_mib: u64) -> u64 {
    (total_mib / 4).max(4096)
}

#[must_use]
pub fn memory_budget_mib(total_mib: u64, available_mib: u64, boxes_resident_mib: u64) -> u64 {
    (available_mib + boxes_resident_mib)
        .min(total_mib)
        .saturating_sub(headroom_mib(total_mib))
}

#[must_use]
pub fn mib(value: i64) -> Option<NonZeroU32> {
    u32::try_from(value).ok().and_then(NonZeroU32::new)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeldDisk {
    pub drives: Vec<String>,
    pub unevictable_mib: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Disk {
    pub total_mib: u64,
    pub free_mib: u64,
    pub written_mib: u64,
}

impl Disk {
    #[must_use]
    pub fn headroom_mib(&self) -> u64 {
        (self.total_mib / 10).max(8192)
    }

    #[must_use]
    pub fn budget_mib(&self) -> u64 {
        (self.free_mib + self.written_mib).saturating_sub(self.headroom_mib())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Size {
    pub memory_mib: NonZeroU32,
    pub disk_mib: NonZeroU32,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Tap {
    index: u8,
    size: Size,
}

impl Tap {
    #[must_use]
    pub fn index(&self) -> u8 {
        self.index
    }

    #[must_use]
    pub fn memory_mib(&self) -> NonZeroU32 {
        self.size.memory_mib
    }

    #[must_use]
    pub fn disk_mib(&self) -> NonZeroU32 {
        self.size.disk_mib
    }

    fn into_parts(self) -> (u8, Size) {
        (self.index, self.size)
    }

    #[must_use]
    pub fn device(&self) -> String {
        format!("tumpel{}", self.index)
    }

    #[must_use]
    pub fn gateway(&self) -> String {
        format!("172.16.{}.1", self.index)
    }

    #[must_use]
    pub fn guest(&self) -> String {
        format!("172.16.{}.2", self.index)
    }

    #[must_use]
    pub fn guest_mac(&self) -> String {
        format!("06:00:ac:10:{:02x}:02", self.index)
    }

    #[must_use]
    pub fn kernel_ip(&self) -> String {
        format!("ip={}::{}:255.255.255.252::eth0:off", self.guest(), self.gateway())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum PoolExhausted {
    Devices,
    Memory {
        boxes: usize,
        held_mib: u64,
        budget_mib: u64,
        requested_mib: NonZeroU32,
    },
    Disk {
        boxes: usize,
        held_mib: u64,
        disk: Disk,
        requested_mib: NonZeroU32,
    },
    Pressure {
        available_mib: u64,
        headroom_mib: u64,
        requested_mib: NonZeroU32,
    },
}

impl fmt::Display for PoolExhausted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Devices => write!(
                formatter,
                "all {TAP_POOL} TAP devices of this host are held by boxes that run a turn or wait for a snapshot of their last turn"
            ),
            Self::Memory {
                boxes,
                held_mib,
                budget_mib,
                requested_mib,
            } => write!(
                formatter,
                "this host's {boxes} boxes hold {held_mib} MiB of the {budget_mib} MiB it gives boxes, so a box of {requested_mib} MiB does not fit; they run a turn or wait for a snapshot of their last turn"
            ),
            Self::Disk {
                boxes,
                held_mib,
                disk,
                requested_mib,
            } => write!(
                formatter,
                "this host gives box disks {} MiB, its {} MiB of free disk and the {} MiB its boxes wrote less the {} MiB it keeps free; its {boxes} boxes hold {held_mib} MiB of it, so a box disk of {requested_mib} MiB does not fit; free disk on the host, or lower the persona's boxDiskMib",
                disk.budget_mib(),
                disk.free_mib,
                disk.written_mib,
                disk.headroom_mib()
            ),
            Self::Pressure {
                available_mib,
                headroom_mib,
                requested_mib,
            } => write!(
                formatter,
                "this host has {available_mib} MiB of memory available and keeps {headroom_mib} MiB of it free, so a box of {requested_mib} MiB does not fit now"
            ),
        }
    }
}

pub struct TapPool {
    held: BTreeSet<u8>,
    budget_mib: u64,
    headroom_mib: u64,
    box_size: Size,
    held_mib: u64,
    held_disk_mib: u64,
}

impl TapPool {
    #[must_use]
    pub fn new(budget_mib: u64, headroom_mib: u64, box_size: Size) -> Self {
        Self {
            held: BTreeSet::new(),
            budget_mib,
            headroom_mib,
            box_size,
            held_mib: 0,
            held_disk_mib: 0,
        }
    }

    #[must_use]
    pub fn size(&self, memory_mib: Option<NonZeroU32>, disk_mib: Option<NonZeroU32>) -> Size {
        Size {
            memory_mib: memory_mib.unwrap_or(self.box_size.memory_mib),
            disk_mib: disk_mib.unwrap_or(self.box_size.disk_mib),
        }
    }

    #[must_use]
    pub fn held_disk_mib(&self) -> u64 {
        self.held_disk_mib
    }

    pub fn admits(&self, size: Size, freeing: &[Size], available_mib: u64, disk: Disk) -> Result<(), PoolExhausted> {
        if self.held.range(..TAP_POOL).count() == usize::from(TAP_POOL) && freeing.is_empty() {
            return Err(PoolExhausted::Devices);
        }
        let memory_mib = size.memory_mib;
        let freed_mib = freeing
            .iter()
            .map(|freed| u64::from(freed.memory_mib.get()))
            .sum::<u64>();
        let freed_disk_mib = freeing.iter().map(|freed| u64::from(freed.disk_mib.get())).sum::<u64>();
        let held_mib = self.held_mib - freed_mib;
        if held_mib == 0 {
            return Ok(());
        }
        if held_mib + u64::from(memory_mib.get()) > self.budget_mib {
            return Err(PoolExhausted::Memory {
                boxes: self.held.len(),
                held_mib: self.held_mib,
                budget_mib: self.budget_mib,
                requested_mib: memory_mib,
            });
        }
        if self.held_disk_mib - freed_disk_mib + u64::from(size.disk_mib.get()) > disk.budget_mib() {
            return Err(PoolExhausted::Disk {
                boxes: self.held.len(),
                held_mib: self.held_disk_mib,
                disk,
                requested_mib: size.disk_mib,
            });
        }
        if u64::from(memory_mib.get()) + self.headroom_mib > available_mib + freed_mib {
            return Err(PoolExhausted::Pressure {
                available_mib,
                headroom_mib: self.headroom_mib,
                requested_mib: memory_mib,
            });
        }
        Ok(())
    }

    pub fn claim(&mut self, size: Size, available_mib: u64, disk: Disk) -> Result<Tap, PoolExhausted> {
        self.admits(size, &[], available_mib, disk)?;
        let index = (0..TAP_POOL)
            .find(|index| !self.held.contains(index))
            .ok_or(PoolExhausted::Devices)?;
        self.held.insert(index);
        self.hold(size);
        Ok(Tap { index, size })
    }

    pub fn claim_device(&mut self, index: i32, size: Size) -> Result<Tap, String> {
        let index = u8::try_from(index)
            .ok()
            .filter(|index| self.held.insert(*index))
            .ok_or_else(|| format!("TAP device tumpel{index} is not a tumpel TAP device or is held by another box"))?;
        self.hold(size);
        Ok(Tap { index, size })
    }

    fn hold(&mut self, size: Size) {
        self.held_mib += u64::from(size.memory_mib.get());
        self.held_disk_mib += u64::from(size.disk_mib.get());
    }

    pub fn release(&mut self, tap: Tap) {
        let (index, size) = tap.into_parts();
        self.held_mib -= u64::from(size.memory_mib.get());
        self.held_disk_mib -= u64::from(size.disk_mib.get());
        self.held.remove(&index);
    }

    #[must_use]
    pub fn capacity(&self) -> NonZeroU32 {
        let boxes = u8::try_from(self.budget_mib / u64::from(self.box_size.memory_mib.get()))
            .map_or(TAP_POOL, |boxes| boxes.min(TAP_POOL));
        NonZeroU32::MIN.saturating_add(u32::from(boxes.saturating_sub(1)))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::{headroom_mib, memory_budget_mib, Disk, PoolExhausted, Size, TapPool};

    fn memory(memory_mib: u32) -> Size {
        Size {
            memory_mib: NonZeroU32::new(memory_mib).expect("a positive size"),
            disk_mib: BOX.disk_mib,
        }
    }

    const PLENTY: u64 = 1 << 30;

    const DEFAULT: NonZeroU32 = NonZeroU32::new(4096).expect("a positive size");

    const BOX: Size = Size {
        memory_mib: DEFAULT,
        disk_mib: NonZeroU32::new(32_768).expect("a positive size"),
    };

    const ROOMY: Disk = Disk {
        total_mib: 1 << 30,
        free_mib: 1 << 29,
        written_mib: 0,
    };

    #[test]
    fn the_58_gib_host_admits_ten_default_boxes_and_leaves_a_quarter_of_its_memory_free() {
        let budget = memory_budget_mib(59_392, 59_392, 0);
        assert_eq!(budget, 44_544);
        let mut pool = TapPool::new(budget, 4096, BOX);
        assert_eq!(pool.capacity().get(), 10);
        let taps: Vec<_> = (0..10)
            .map(|_| pool.claim(BOX, PLENTY, ROOMY).expect("a default box"))
            .collect();
        assert_eq!(
            pool.claim(BOX, PLENTY, ROOMY),
            Err(PoolExhausted::Memory {
                boxes: 10,
                held_mib: 40_960,
                budget_mib: 44_544,
                requested_mib: DEFAULT
            })
        );
        assert_eq!(taps[9].device(), "tumpel9");
    }

    #[test]
    fn a_host_short_of_memory_now_refuses_a_box_its_budget_still_admits() {
        let headroom = headroom_mib(59_392);
        assert_eq!(headroom, 14_848);
        let mut pool = TapPool::new(44_544, headroom, BOX);
        let taps: Vec<_> = (0..9)
            .map(|_| pool.claim(BOX, PLENTY, ROOMY).expect("a default box"))
            .collect();
        assert_eq!(
            pool.claim(BOX, 2048, ROOMY),
            Err(PoolExhausted::Pressure {
                available_mib: 2048,
                headroom_mib: 14_848,
                requested_mib: DEFAULT
            })
        );
        assert_eq!(
            pool.admits(BOX, &[], 2048, ROOMY).map_err(|refused| refused.to_string()),
            Err("this host has 2048 MiB of memory available and keeps 14848 MiB of it free, so a box of 4096 MiB does not fit now".to_string())
        );
        assert!(pool.admits(BOX, &[BOX], 14_848, ROOMY).is_ok());
        assert!(pool.claim(BOX, 18_944, ROOMY).is_ok());
        assert_eq!(taps.len(), 9);
        let mut empty = TapPool::new(44_544, headroom, BOX);
        assert!(empty.claim(BOX, 0, ROOMY).is_ok());
    }

    #[test]
    fn the_budget_counts_only_memory_other_programs_leave_available() {
        assert_eq!(memory_budget_mib(59_392, 40_000, 0), 25_152);
        assert_eq!(memory_budget_mib(8192, 6000, 0), 1904);
        assert_eq!(memory_budget_mib(8192, 2048, 0), 0);
    }

    #[test]
    fn a_handover_with_running_boxes_admits_by_the_memory_other_programs_leave_free() {
        let fresh = memory_budget_mib(59_392, 42_032, 0);
        assert_eq!(fresh, 27_184);
        assert_eq!(TapPool::new(fresh, 4096, BOX).capacity().get(), 6);
        let handed_resident = 5 * 3584;
        let after = memory_budget_mib(59_392, 42_032 - handed_resident, handed_resident);
        assert_eq!(after, fresh);
        let mut pool = TapPool::new(after, 4096, BOX);
        assert_eq!(pool.capacity().get(), 6);
        let handed: Vec<_> = (0..5)
            .map(|index| pool.claim_device(index, BOX).expect("a handed box"))
            .collect();
        let sixth = pool
            .claim(BOX, PLENTY, ROOMY)
            .expect("the sixth box fits beside the five handed ones");
        assert_eq!(sixth.device(), "tumpel5");
        assert_eq!(
            pool.claim(BOX, PLENTY, ROOMY),
            Err(PoolExhausted::Memory {
                boxes: 6,
                held_mib: 24_576,
                budget_mib: 27_184,
                requested_mib: DEFAULT
            })
        );
        assert_eq!(handed.len(), 5);
    }

    #[test]
    fn a_handover_adopts_every_box_whatever_its_device_and_admits_new_ones_once_they_fit() {
        let mut pool = TapPool::new(8192, 4096, BOX);
        let handed: Vec<_> = [11, 70, 255]
            .into_iter()
            .map(|index| pool.claim_device(index, BOX).expect("a handed box"))
            .collect();
        assert_eq!(
            pool.claim(BOX, PLENTY, ROOMY),
            Err(PoolExhausted::Memory {
                boxes: 3,
                held_mib: 12_288,
                budget_mib: 8192,
                requested_mib: DEFAULT
            })
        );
        assert!(pool.claim_device(70, BOX).is_err());
        assert!(pool.claim_device(256, BOX).is_err());
        let mut handed = handed.into_iter();
        pool.release(handed.next().expect("tumpel11"));
        pool.release(handed.next().expect("tumpel70"));
        assert_eq!(
            pool.claim(BOX, PLENTY, ROOMY)
                .expect("a box fits beside one handed box")
                .device(),
            "tumpel0"
        );
    }

    #[test]
    fn boxes_are_admitted_while_their_memory_fits_and_an_empty_host_admits_any_box() {
        let mut pool = TapPool::new(12_288, 4096, BOX);
        let first = pool.claim(BOX, PLENTY, ROOMY).expect("first default box");
        let second = pool.claim(memory(8192), PLENTY, ROOMY).expect("a box asking for 8 GiB");
        assert_eq!(
            pool.claim(memory(128), PLENTY, ROOMY),
            Err(PoolExhausted::Memory {
                boxes: 2,
                held_mib: 12_288,
                budget_mib: 12_288,
                requested_mib: NonZeroU32::new(128).expect("128")
            })
        );
        assert_eq!(
            pool.admits(memory(8192), &[BOX], PLENTY, ROOMY),
            Err(PoolExhausted::Memory {
                boxes: 2,
                held_mib: 12_288,
                budget_mib: 12_288,
                requested_mib: NonZeroU32::new(8192).expect("8192")
            })
        );
        assert_eq!(pool.admits(memory(8192), &[memory(8192)], PLENTY, ROOMY), Ok(()));
        pool.release(second);
        pool.release(first);
        let large = pool
            .claim(memory(65_536), PLENTY, ROOMY)
            .expect("an empty host admits a box larger than its budget");
        assert_eq!(
            (large.device(), large.memory_mib().get()),
            ("tumpel0".to_string(), 65_536)
        );
        assert!(pool.claim(memory(128), PLENTY, ROOMY).is_err());
    }

    #[test]
    fn a_host_out_of_devices_says_so_and_a_freed_device_admits_the_next_box() {
        let mut pool = TapPool::new(1_048_576, 4096, BOX);
        let taps: Vec<_> = (0..64)
            .map(|_| pool.claim(BOX, PLENTY, ROOMY).expect("a device"))
            .collect();
        assert_eq!(pool.claim(BOX, PLENTY, ROOMY), Err(PoolExhausted::Devices));
        assert_eq!(
            PoolExhausted::Devices.to_string(),
            "all 64 TAP devices of this host are held by boxes that run a turn or wait for a snapshot of their last turn"
        );
        assert_eq!(pool.admits(BOX, &[BOX], PLENTY, ROOMY), Ok(()));
        assert_eq!(pool.capacity().get(), 64);
        let mut taps = taps.into_iter();
        pool.release(taps.next().expect("the first device"));
        assert_eq!(
            pool.claim(BOX, PLENTY, ROOMY).expect("the returned device").device(),
            "tumpel0"
        );
    }

    #[test]
    fn a_reattached_box_is_charged_the_memory_it_booted_with() {
        let mut pool = TapPool::new(12_288, 4096, BOX);
        let handed = pool.claim_device(5, memory(8192)).expect("device 5");
        assert_eq!(handed.device(), "tumpel5");
        assert!(pool.claim_device(5, BOX).is_err());
        assert!(pool.claim(memory(8192), PLENTY, ROOMY).is_err());
        pool.release(handed);
        assert!(pool.claim(memory(8192), PLENTY, ROOMY).is_ok());
    }

    #[test]
    fn box_disks_are_admitted_by_their_full_size_not_by_what_they_wrote() {
        let mut pool = TapPool::new(1_048_576, 4096, BOX);
        let fresh = Disk {
            total_mib: 512_000,
            free_mib: 200_000,
            written_mib: 0,
        };
        assert_eq!(fresh.budget_mib(), 148_800);
        let taps: Vec<_> = (0..4)
            .map(|_| pool.claim(BOX, PLENTY, fresh).expect("a box disk"))
            .collect();
        let written = Disk {
            total_mib: 512_000,
            free_mib: 150_000,
            written_mib: 50_000,
        };
        assert_eq!(
            pool.claim(BOX, PLENTY, written),
            Err(PoolExhausted::Disk {
                boxes: 4,
                held_mib: 131_072,
                disk: written,
                requested_mib: BOX.disk_mib
            })
        );
        assert_eq!(
            pool.admits(BOX, &[], PLENTY, written).map_err(|refused| refused.to_string()),
            Err("this host gives box disks 148800 MiB, its 150000 MiB of free disk and the 50000 MiB its boxes wrote less the 51200 MiB it keeps free; its 4 boxes hold 131072 MiB of it, so a box disk of 32768 MiB does not fit; free disk on the host, or lower the persona's boxDiskMib".to_string())
        );
        assert_eq!(pool.admits(BOX, &[BOX], PLENTY, written), Ok(()));
        assert_eq!(pool.held_disk_mib(), 131_072);
        let mut taps = taps.into_iter();
        pool.release(taps.next().expect("the first box"));
        assert_eq!(pool.held_disk_mib(), 98_304);
        assert!(pool.claim(BOX, PLENTY, written).is_ok());
    }

    #[test]
    fn an_empty_host_admits_a_box_disk_larger_than_its_disk_budget() {
        let small = Disk {
            total_mib: 40_000,
            free_mib: 20_000,
            written_mib: 0,
        };
        assert_eq!(small.headroom_mib(), 8192);
        assert_eq!(small.budget_mib(), 11_808);
        let mut pool = TapPool::new(1_048_576, 4096, BOX);
        let only = pool.claim(BOX, PLENTY, small).expect("an empty host admits any box");
        assert_eq!(only.disk_mib(), BOX.disk_mib);
        assert!(matches!(
            pool.claim(BOX, PLENTY, small),
            Err(PoolExhausted::Disk { boxes: 1, .. })
        ));
    }

    #[test]
    fn each_device_carries_its_own_slash_30() {
        let mut pool = TapPool::new(1_048_576, 4096, BOX);
        let taps: Vec<_> = (0..12).map(|_| pool.claim(BOX, PLENTY, ROOMY).expect("tap")).collect();
        let last = &taps[11];
        assert_eq!(last.device(), "tumpel11");
        assert_eq!(last.gateway(), "172.16.11.1");
        assert_eq!(last.guest_mac(), "06:00:ac:10:0b:02");
        assert_eq!(
            last.kernel_ip(),
            "ip=172.16.11.2::172.16.11.1:255.255.255.252::eth0:off"
        );
    }
}
