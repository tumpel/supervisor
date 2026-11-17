use std::collections::BTreeSet;
use std::fmt;

pub const MAX_POOL_SIZE: u32 = 256;

#[derive(Debug, PartialEq, Eq)]
pub struct Tap {
    index: u8,
}

impl Tap {
    #[must_use]
    pub fn index(&self) -> u8 {
        self.index
    }

    fn into_index(self) -> u8 {
        self.index
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
pub struct PoolExhausted {
    pub size: u32,
}

impl fmt::Display for PoolExhausted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "all {} boxes held (TAP devices tumpel0 to tumpel{}) by running boxes or by paused boxes whose last turn is not in a snapshot yet; wait for a box to finish, or raise boxCapacity in runtime.json and restart tumpel-supervisor",
            self.size,
            self.size.saturating_sub(1)
        )
    }
}

pub struct TapPool {
    size: u32,
    free: BTreeSet<u8>,
}

impl TapPool {
    pub fn new(size: u32) -> Result<Self, String> {
        if size == 0 || size > MAX_POOL_SIZE {
            return Err(format!(
                "boxCapacity {size} is outside 1 to {MAX_POOL_SIZE}: device tumpel<k> routes 172.16.<k>.0/30, so k stays below {MAX_POOL_SIZE}; set boxCapacity in runtime.json and restart tumpel-supervisor"
            ));
        }
        let free = (0..size).filter_map(|index| u8::try_from(index).ok()).collect();
        Ok(Self { size, free })
    }

    pub fn claim(&mut self) -> Result<Tap, PoolExhausted> {
        self.free
            .pop_first()
            .map(|index| Tap { index })
            .ok_or(PoolExhausted { size: self.size })
    }

    pub fn claim_device(&mut self, index: i32) -> Result<Tap, String> {
        u8::try_from(index)
            .ok()
            .filter(|index| self.free.remove(index))
            .map(|index| Tap { index })
            .ok_or_else(|| {
                format!(
                    "TAP device tumpel{index} is outside this host's {} boxes or held by another box",
                    self.size
                )
            })
    }

    pub fn release(&mut self, tap: Tap) {
        self.free.insert(tap.into_index());
    }

    #[must_use]
    pub fn free(&self) -> usize {
        self.free.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{PoolExhausted, TapPool};

    #[test]
    fn claims_hand_out_the_lowest_free_device_and_refuse_past_the_pool() {
        let mut pool = TapPool::new(2).expect("pool");
        let first = pool.claim().expect("first");
        let second = pool.claim().expect("second");
        assert_eq!(
            (first.device(), second.device()),
            ("tumpel0".to_string(), "tumpel1".to_string())
        );
        assert_eq!(pool.claim(), Err(PoolExhausted { size: 2 }));
        pool.release(first);
        let again = pool.claim().expect("returned device");
        assert_eq!(again.device(), "tumpel0");
        assert_eq!(pool.free(), 0);
        pool.release(again);
        pool.release(second);
        assert_eq!(pool.free(), 2);
    }

    #[test]
    fn each_device_carries_its_own_slash_30() {
        let mut pool = TapPool::new(12).expect("pool");
        let taps: Vec<_> = (0..12).map(|_| pool.claim().expect("tap")).collect();
        let last = &taps[11];
        assert_eq!(last.device(), "tumpel11");
        assert_eq!(last.gateway(), "172.16.11.1");
        assert_eq!(last.guest_mac(), "06:00:ac:10:0b:02");
        assert_eq!(
            last.kernel_ip(),
            "ip=172.16.11.2::172.16.11.1:255.255.255.252::eth0:off"
        );
    }

    #[test]
    fn a_pool_outside_the_172_16_range_is_refused() {
        assert!(TapPool::new(0).is_err());
        assert!(TapPool::new(257).is_err());
        assert_eq!(TapPool::new(256).expect("full range").free(), 256);
    }
}
