/// Basic state shared across all implementations of a Scan.
pub const ScanState = enum {
    /// The scan has not been executed yet.
    idle,

    /// The scan is at a valid position and ready to yield values.
    seeking,

    /// The scan needs to load data from storage.
    needs_data,

    /// The scan is attempting to load data from storage.
    buffering,
};
