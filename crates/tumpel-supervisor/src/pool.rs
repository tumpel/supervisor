use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU32;

pub const TAP_POOL: u8 = 64;

#[must_use]
pub fn headroom_mib(total_mib: u64) -> u64 {
    (total_mib / 4).max(4096)
}

#[must_use]
pub fn memory_budget_mib(total_mib: u64) -> u64 {
    total_mib.saturating_sub(headroom_mib(total_mib))
}

#[must_use]
pub fn mib(value: i64) -> Option<NonZeroU32> {
    u32::try_from(value).ok().and_then(NonZeroU32::new)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Held {
    pub drives: Vec<String>,
    pub disk_held_mib: u64,
    pub memory_budget_mib: u64,
    pub memory_held_mib: u64,
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
}

impl fmt::Display for PoolExhausted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Devices => write!(
                formatter,
                "all {TAP_POOL} TAP devices of this host are held by boxes"
            ),
            Self::Memory {
                boxes,
                held_mib,
                budget_mib,
                requested_mib,
            } => write!(
                formatter,
                "this host's {boxes} boxes hold {held_mib} MiB of the {budget_mib} MiB it gives boxes, so a box of {requested_mib} MiB does not fit"
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
        }
    }
}

pub struct TapPool {
    held: BTreeSet<u8>,
    budget_mib: u64,
    box_size: Size,
    held_mib: u64,
    held_disk_mib: u64,
}

impl TapPool {
    #[must_use]
    pub fn new(budget_mib: u64, box_size: Size) -> Self {
        Self {
            held: BTreeSet::new(),
            budget_mib,
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

    #[must_use]
    pub fn budget_mib(&self) -> u64 {
        self.budget_mib
    }

    #[must_use]
    pub fn held_mib(&self) -> u64 {
        self.held_mib
    }

    fn admits(&self, size: Size, disk: Disk) -> Result<(), PoolExhausted> {
        if self.held.range(..TAP_POOL).count() == usize::from(TAP_POOL) {
            return Err(PoolExhausted::Devices);
        }
        let memory_mib = size.memory_mib;
        if self.held_mib + u64::from(memory_mib.get()) > self.budget_mib {
            return Err(PoolExhausted::Memory {
                boxes: self.held.len(),
                held_mib: self.held_mib,
                budget_mib: self.budget_mib,
                requested_mib: memory_mib,
            });
        }
        if self.held_mib == 0 {
            return Ok(());
        }
        if self.held_disk_mib + u64::from(size.disk_mib.get()) > disk.budget_mib() {
            return Err(PoolExhausted::Disk {
                boxes: self.held.len(),
                held_mib: self.held_disk_mib,
                disk,
                requested_mib: size.disk_mib,
            });
        }
        Ok(())
    }

    pub fn claim(&mut self, size: Size, disk: Disk) -> Result<Tap, PoolExhausted> {
        self.admits(size, disk)?;
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
    fn a_44_gib_host_refuses_the_box_that_would_pass_its_budget_at_configured_size() {
        let budget = memory_budget_mib(45_056);
        assert_eq!(budget, 33_792);
        let mut pool = TapPool::new(budget, BOX);
        assert_eq!(pool.capacity().get(), 8);
        let taps: Vec<_> = (0..8)
            .map(|_| {
                pool.claim(pool.size(None, None), ROOMY)
                    .expect("a box at the configured 4 GiB")
            })
            .collect();
        assert_eq!(
            pool.claim(pool.size(None, None), ROOMY)
                .map_err(|refused| refused.to_string()),
            Err(
                "this host's 8 boxes hold 32768 MiB of the 33792 MiB it gives boxes, so a box of 4096 MiB does not fit"
                    .to_string()
            )
        );
        assert!(pool.claim(memory(1024), ROOMY).is_ok());
        assert_eq!(taps.len(), 8);
    }

    #[test]
    fn the_budget_is_total_memory_less_the_headroom_whatever_is_free() {
        assert_eq!(headroom_mib(59_392), 14_848);
        assert_eq!(memory_budget_mib(59_392), 44_544);
        assert_eq!(memory_budget_mib(8192), 4096);
        assert_eq!(memory_budget_mib(4096), 0);
    }

    #[test]
    fn a_handover_adopts_every_box_whatever_its_device_and_admits_new_ones_once_they_fit() {
        let mut pool = TapPool::new(8192, BOX);
        let handed: Vec<_> = [11, 70, 255]
            .into_iter()
            .map(|index| pool.claim_device(index, BOX).expect("a handed box"))
            .collect();
        assert_eq!(
            pool.claim(BOX, ROOMY),
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
            pool.claim(BOX, ROOMY)
                .expect("a box fits beside one handed box")
                .device(),
            "tumpel0"
        );
    }

    #[test]
    fn boxes_are_admitted_while_their_memory_fits_the_budget_and_the_first_box_too() {
        let mut pool = TapPool::new(12_288, BOX);
        let first = pool.claim(BOX, ROOMY).expect("first default box");
        let second = pool.claim(memory(8192), ROOMY).expect("a box asking for 8 GiB");
        assert_eq!(
            pool.claim(memory(128), ROOMY),
            Err(PoolExhausted::Memory {
                boxes: 2,
                held_mib: 12_288,
                budget_mib: 12_288,
                requested_mib: NonZeroU32::new(128).expect("128")
            })
        );
        pool.release(second);
        pool.release(first);
        let mut small = TapPool::new(memory_budget_mib(8192), BOX);
        assert_eq!(
            small
                .claim(memory(65_536), ROOMY)
                .map_err(|refused| refused.to_string()),
            Err(
                "this host's 0 boxes hold 0 MiB of the 4096 MiB it gives boxes, so a box of 65536 MiB does not fit"
                    .to_string()
            )
        );
        assert!(small.claim(BOX, ROOMY).is_ok());
    }

    #[test]
    fn a_host_out_of_devices_says_so_and_a_freed_device_admits_the_next_box() {
        let mut pool = TapPool::new(1_048_576, BOX);
        let taps: Vec<_> = (0..64).map(|_| pool.claim(BOX, ROOMY).expect("a device")).collect();
        assert_eq!(pool.claim(BOX, ROOMY), Err(PoolExhausted::Devices));
        assert_eq!(
            PoolExhausted::Devices.to_string(),
            "all 64 TAP devices of this host are held by boxes"
        );
        assert_eq!(pool.capacity().get(), 64);
        let mut taps = taps.into_iter();
        pool.release(taps.next().expect("the first device"));
        assert_eq!(pool.claim(BOX, ROOMY).expect("the returned device").device(), "tumpel0");
    }

    #[test]
    fn a_reattached_box_is_charged_the_memory_it_booted_with() {
        let mut pool = TapPool::new(12_288, BOX);
        let handed = pool.claim_device(5, memory(8192)).expect("device 5");
        assert_eq!(handed.device(), "tumpel5");
        assert!(pool.claim_device(5, BOX).is_err());
        assert!(pool.claim(memory(8192), ROOMY).is_err());
        pool.release(handed);
        assert!(pool.claim(memory(8192), ROOMY).is_ok());
    }

    #[test]
    fn box_disks_are_admitted_by_their_full_size_not_by_what_they_wrote() {
        let mut pool = TapPool::new(1_048_576, BOX);
        let fresh = Disk {
            total_mib: 512_000,
            free_mib: 200_000,
            written_mib: 0,
        };
        assert_eq!(fresh.budget_mib(), 148_800);
        let taps: Vec<_> = (0..4).map(|_| pool.claim(BOX, fresh).expect("a box disk")).collect();
        let written = Disk {
            total_mib: 512_000,
            free_mib: 150_000,
            written_mib: 50_000,
        };
        assert_eq!(
            pool.claim(BOX, written),
            Err(PoolExhausted::Disk {
                boxes: 4,
                held_mib: 131_072,
                disk: written,
                requested_mib: BOX.disk_mib
            })
        );
        assert_eq!(
            pool.claim(BOX, written).map_err(|refused| refused.to_string()),
            Err("this host gives box disks 148800 MiB, its 150000 MiB of free disk and the 50000 MiB its boxes wrote less the 51200 MiB it keeps free; its 4 boxes hold 131072 MiB of it, so a box disk of 32768 MiB does not fit; free disk on the host, or lower the persona's boxDiskMib".to_string())
        );
        assert_eq!(pool.held_disk_mib(), 131_072);
        let mut taps = taps.into_iter();
        pool.release(taps.next().expect("the first box"));
        assert_eq!(pool.held_disk_mib(), 98_304);
        assert!(pool.claim(BOX, written).is_ok());
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
        let mut pool = TapPool::new(1_048_576, BOX);
        let only = pool.claim(BOX, small).expect("an empty host admits any box");
        assert_eq!(only.disk_mib(), BOX.disk_mib);
        assert!(matches!(
            pool.claim(BOX, small),
            Err(PoolExhausted::Disk { boxes: 1, .. })
        ));
    }

    #[test]
    fn each_device_carries_its_own_slash_30() {
        let mut pool = TapPool::new(1_048_576, BOX);
        let taps: Vec<_> = (0..12).map(|_| pool.claim(BOX, ROOMY).expect("tap")).collect();
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
