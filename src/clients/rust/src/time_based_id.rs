use std::cmp::Ordering;
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
///   sequentially starting at the Unix epoch (plus a base random number).
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
    ms_since_epoch: u128,
    random: u128, // 80 bits
}

impl TbidGenerator {
    fn new() -> TbidGenerator {
        let ms_since_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);

        TbidGenerator {
            ms_since_epoch,
            random: random_u80(),
        }
    }

    fn next(&mut self) -> u128 {
        self.next_from_system_time(SystemTime::now())
    }

    fn next_from_system_time(&mut self, now: SystemTime) -> u128 {
        *self = self.next_state_from_system_time(now);

        // Pack `ms_since_epoch` and `random` into a `u128`.
        //
        // |----------|    |----------------|
        //  Timestamp          Randomness
        //    48bits             80bits

        assert!(is_u80(self.random));

        self.ms_since_epoch << 80 | self.random
    }

    fn next_state_from_system_time(&self, now: SystemTime) -> TbidGenerator {
        let previous_ms_since_epoch = self.ms_since_epoch;

        let next_ms_since_epoch = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);

        match next_ms_since_epoch.cmp(&previous_ms_since_epoch) {
            Ordering::Greater => {
                // The new time is greater than previous.
                // Use it and choose a new random number.
                TbidGenerator {
                    ms_since_epoch: next_ms_since_epoch,
                    random: random_u80(),
                }
            }
            Ordering::Equal | Ordering::Less => {
                // The new time is equal or less than previous.
                // Use the old time and increment the random number.
                match self.next_random() {
                    Some(next_random) => TbidGenerator {
                        ms_since_epoch: previous_ms_since_epoch,
                        random: next_random,
                    },
                    None => {
                        // Difficult case.
                        //
                        // `self.random` would overflow a `u80`.
                        //
                        // It _seems_ to be extremely unlikely: it requires many
                        // ids to be generated in a single millisecond, and also
                        // for the initial value of `self.random` to be large;
                        // though consider that the clock may be broken and a
                        // "millisecond" could last forever.
                        //
                        // The ULID spec says if the randomness overflows 80
                        // bits then it is an error, but we instead manually
                        // carry the bit from the randomness to the timestamp,
                        // moving time forward by 1ms, and generate a new random
                        // number. Generated ids will be ahead of the wall
                        // clock by 1 ms until time catches up.
                        TbidGenerator {
                            // There is too much resolution in a u128 for this to ever overflow.
                            ms_since_epoch: previous_ms_since_epoch
                                .checked_add(1)
                                .expect("impossible overflow"),
                            random: random_u80(),
                        }
                    }
                }
            }
        }
    }

    fn next_random(&self) -> Option<u128> {
        assert!(is_u80(self.random));
        if self.random == U80_MASK {
            None
        } else {
            Some(self.random + 1)
        }
    }
}

const U80_MASK: u128 = 0x_FFFF_FFFF_FFFF_FFFF_FFFF;

fn is_u80(val: u128) -> bool {
    val <= U80_MASK
}

fn random_u80() -> u128 {
    random_u128() & U80_MASK
}

fn random_u128() -> u128 {
    let a = random_u64();
    let b = random_u64();
    let a = a as u128;
    let b = b as u128;
    a | (b << 64)
}

fn random_u64() -> u64 {
    std::hash::Hasher::finish(&std::hash::BuildHasher::build_hasher(
        &std::collections::hash_map::RandomState::new(),
    ))
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant, SystemTime};

    use super::{id, TbidGenerator, U80_MASK};

    struct TestResult {
        id_start: u128,
        id_end: u128,
    }

    fn run_test_for_a_few_ms(mut id_generator: impl FnMut() -> u128) -> TestResult {
        // Enough time to transition between millisecond timestamps
        let test_duration = Duration::from_millis(10);
        let end_instant = Instant::now() + test_duration;

        let mut id_prev = 0;
        let mut id_start = None;
        let mut id_end = None;

        while Instant::now() < end_instant {
            let id_next = id_generator();

            assert!(id_prev < id_next, "ids not monotonically increasing");

            // Regression test - should not reset random to 0 on overflow,
            // but instead generate a new random number.
            let id_next_random_part = id_next & U80_MASK;
            assert_ne!(id_next_random_part, 0);

            // Don't waste other processes' time
            std::thread::yield_now();

            id_start = id_start.or(Some(id_next));
            id_end = Some(id_next);

            id_prev = id_next
        }

        let id_start = id_start.unwrap();
        let id_end = id_end.unwrap();

        TestResult { id_start, id_end }
    }

    fn assert_ids_monotonic_and_multiple_timestamps(id_generator: impl FnMut() -> u128) {
        let TestResult { id_start, id_end } = run_test_for_a_few_ms(id_generator);
        // Verify we transitioned between timestamps
        let timestamp_start = id_start & !U80_MASK;
        let timestamp_end = id_end & !U80_MASK;
        assert_ne!(timestamp_start, timestamp_end);
    }

    fn assert_ids_monotonic_and_single_timestamp(id_generator: impl FnMut() -> u128) {
        let TestResult { id_start, id_end } = run_test_for_a_few_ms(id_generator);
        // Verify we transitioned between timestamps
        let timestamp_start = id_start & !U80_MASK;
        let timestamp_end = id_end & !U80_MASK;
        assert_eq!(timestamp_start, timestamp_end);
    }

    #[test]
    fn test_ids_with_normal_clock() {
        assert_ids_monotonic_and_multiple_timestamps(|| id());
    }

    #[test]
    fn test_random_overflow_still_monotonic() {
        // Put this test in the future so the ms don't increment
        let future_duration_from_now = Duration::from_secs(1_000_000);
        let future_time = SystemTime::now()
            .checked_add(future_duration_from_now)
            .unwrap();
        let future_duration_since_epoch = future_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let mut idgen = TbidGenerator {
            ms_since_epoch: future_duration_since_epoch,
            random: U80_MASK - 10,
        };

        assert_ids_monotonic_and_multiple_timestamps(|| idgen.next());
    }

    #[test]
    fn test_reverse_time() {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let mut idgen = TbidGenerator {
            ms_since_epoch: now,
            random: 0,
        };

        let mut count = 0;

        assert_ids_monotonic_and_single_timestamp(|| {
            let past_time_base = SystemTime::UNIX_EPOCH - Duration::from_secs(1_000_000);
            let past_time = past_time_base + Duration::from_millis(count);
            count += 1;
            idgen.next_from_system_time(past_time)
        });
    }

    #[test]
    fn test_reverse_time_then_catch_up() {
        let now = SystemTime::now();
        let now_ms = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let mut idgen = TbidGenerator {
            ms_since_epoch: now_ms,
            random: 0,
        };

        let mut count = 0;

        assert_ids_monotonic_and_multiple_timestamps(|| {
            let past_time_base = now - Duration::from_millis(2);
            let past_time = past_time_base + Duration::from_millis(count);
            count += 1;
            idgen.next_from_system_time(past_time)
        });
    }

    #[test]
    fn test_prior_to_unix_epoch() {
        let mut idgen = TbidGenerator {
            ms_since_epoch: 0,
            random: 0,
        };

        let mut count = 0;

        assert_ids_monotonic_and_single_timestamp(|| {
            let past_time_base = SystemTime::UNIX_EPOCH - Duration::from_secs(1_000_000);
            let past_time = past_time_base + Duration::from_millis(count);
            count += 1;
            idgen.next_from_system_time(past_time)
        });
    }
}
