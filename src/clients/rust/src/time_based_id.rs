use std::sync::Mutex;
use std::time::SystemTime;

/// Generate a TigerBeetle time-based identifier.
///
/// This generates `u128` identifiers suitable for the `id` fields
/// of TigerBeetle `Account`s and `Transaction`s.
///
/// [TigerBeetle time-based identifiers][tbid] include a timestamp
/// component and a random component, are lexicographically sortable,
/// monotonically increasing, and enable optimizations in TigerBeetle's LSM tree.
///
/// [tbid]: https://docs.tigerbeetle.com/coding/data-modeling/#tigerbeetle-time-based-identifiers-recommended
///
/// ## System clock warning
///
/// Generating these IDs correctly depends on a well-behaved system clock. There
/// are two cases that will cause degenerate behavior:
///
/// - The system clock changes into the past. IDs will be generated
///   sequentially from the previous ID until the clock catches up with the future timestamp.
///
/// - The system time is prior to the Unix epoch. IDs will be generated
///   sequentially starting at 1, with a timestamp at the Unix epoch.
//
// References:
//
// - https://github.com/tigerbeetle/tigerbeetle/blob/75f77b8b3280ce2f289cf42ae928945190fe4a2a/src/clients/node/src/index.ts#L161-L191
// - https://github.com/ulid/spec
pub fn id() -> u128 {
    let mut guard = GLOBAL_GENERATOR.lock().expect("global tbid generator");
    match *guard {
        None => {
            *guard = Some(TbidGenerator::new());
            drop(guard);
            id()
        }
        Some(ref mut generator) => generator.next(),
    }
}

static GLOBAL_GENERATOR: Mutex<Option<TbidGenerator>> = Mutex::new(None);

struct TbidGenerator {
    previous: u128,
}

impl TbidGenerator {
    fn new() -> TbidGenerator {
        TbidGenerator { previous: 0 }
    }

    fn next(&mut self) -> u128 {
        self.next_from_time(SystemTime::now())
    }

    fn next_from_time(&mut self, time: SystemTime) -> u128 {
        let time_ms_new = Self::time_ms_shifted(time);

        if time_ms_new > self.previous {
            self.previous = time_ms_new | Self::entropy();
        } else {
            // Same/backward time: increment, carrying into the timestamp on entropy overflow.
            self.previous = self.previous.checked_add(1).expect("time overflow");
        }

        self.previous
    }

    fn time_ms_shifted(time: SystemTime) -> u128 {
        let time_ms = time
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        assert!(time_ms <= u128::MAX >> 80, "time overflow");
        time_ms << 80
    }

    fn entropy() -> u128 {
        let msb: u128 = random_u64().into();
        let lsb: u16 = random_u64() as u16;

        let result = msb << 16 | u128::from(lsb);

        assert!(result <= U80_MASK);
        result
    }
}

const U80_MASK: u128 = 0x_FFFF_FFFF_FFFF_FFFF_FFFF;

fn random_u64() -> u64 {
    std::hash::Hasher::finish(&std::hash::BuildHasher::build_hasher(
        &std::collections::hash_map::RandomState::new(),
    ))
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use super::{id, TbidGenerator, U80_MASK};

    fn time_from_ms(ms: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(ms)
    }

    #[test]
    fn test_ids_with_normal_clock() {
        let mut previous = 0;
        for _ in 0..1_000 {
            let next = id();
            assert!(previous < next, "ids not monotonically increasing");
            previous = next;
        }
    }

    #[test]
    fn test_random_overflow_still_monotonic() {
        let initial = (100 << 80) | (U80_MASK - 10);
        let mut idgen = TbidGenerator { previous: initial };

        for increment in 1..=12 {
            // Simulated time is stuck at 100, so we increment and overflow the random part.
            let next = idgen.next_from_time(time_from_ms(100));
            assert_eq!(next, initial + increment);
        }

        // Once system time is un-stuck, we continue with the same random part.
        assert_eq!(idgen.next_from_time(time_from_ms(101)), initial + 13);
        let next = idgen.next_from_time(time_from_ms(102));
        assert_eq!(next >> 80, 102);
        assert!(next > initial + 13);
    }

    #[test]
    fn test_reverse_time() {
        let initial = (100 << 80) | 42;
        let mut idgen = TbidGenerator { previous: initial };

        for reverse_ms in 1..=100 {
            // We have to go back Marty! Back to the future!
            let next = idgen.next_from_time(time_from_ms(100 - reverse_ms));
            assert_eq!(next, initial + u128::from(reverse_ms));
        }
    }

    #[test]
    fn test_reverse_time_then_catch_up() {
        let initial: u128 = (100 << 80) | 42;
        let mut idgen = TbidGenerator { previous: initial };
        // We started at 100, but reset to 98, and catch up from there
        assert_eq!(idgen.next_from_time(time_from_ms(98)), initial + 1);
        assert_eq!(idgen.next_from_time(time_from_ms(99)), initial + 2);
        assert_eq!(idgen.next_from_time(time_from_ms(100)), initial + 3);
        // We're back to the future, and get a new random part
        let caught_up = idgen.next_from_time(time_from_ms(101));
        assert_eq!(caught_up >> 80, 101);
        assert!(caught_up > initial + 3);
    }

    #[test]
    fn test_prior_to_unix_epoch() {
        let mut idgen = TbidGenerator::new();
        let past_time = SystemTime::UNIX_EPOCH - Duration::from_secs(1);

        for expected in 1..=100 {
            assert_eq!(idgen.next_from_time(past_time), expected);
        }
        assert_eq!(idgen.next_from_time(SystemTime::UNIX_EPOCH), 101);
        let next = idgen.next_from_time(time_from_ms(1));
        assert_eq!(next >> 80, 1);
        assert!(next > 101);
    }

    #[test]
    fn test_same_millisecond() {
        let mut idgen = TbidGenerator::new();
        let tbid_start = idgen.next_from_time(time_from_ms(100));
        assert_eq!(tbid_start >> 80, 100);
        assert_eq!(idgen.previous, tbid_start);

        for elapsed_micros in 1..=100 {
            let time_new = time_from_ms(100) + Duration::from_micros(elapsed_micros);
            assert_eq!(
                idgen.next_from_time(time_new),
                tbid_start + u128::from(elapsed_micros)
            );
        }
    }

    #[test]
    #[should_panic(expected = "time overflow")]
    fn test_timestamp_overflow() {
        TbidGenerator::time_ms_shifted(time_from_ms(1 << 48));
    }

    #[test]
    #[should_panic(expected = "time overflow")]
    fn test_id_overflow() {
        let mut idgen = TbidGenerator {
            previous: u128::MAX,
        };
        idgen.next_from_time(time_from_ms(100));
    }
}
