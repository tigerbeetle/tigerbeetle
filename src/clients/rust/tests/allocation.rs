use futures::executor::block_on;
use std::cell::RefCell;

use tigerbeetle as tb;

mod test_db;
pub use test_db::*;

thread_local! {
    // Each test has the main thread and an IO thread, whose thread id is opaque to us.
    // Allocation and deallocation count may therefore not match, e.g., if something is
    // allocated on the I/O thread and then passed to the test thread; consequently, we
    // check output buffer growth on the I/O thread using pointer and capacity equality.
    static ALLOCATION_STATS: RefCell<AllocationStats> = RefCell::new(Default::default());
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator {};

struct TrackingAllocator {}

#[derive(Default, Clone, Debug)]
struct AllocationStats {
    count_alloc: usize,
    count_dealloc: usize,
    bytes_alloc: usize,
    bytes_dealloc: usize,
}

impl AllocationStats {
    fn snapshot() -> AllocationStats {
        ALLOCATION_STATS.with(|stats| stats.borrow().clone())
    }

    fn diff(&self) -> AllocationStats {
        let stats_old = self;
        let stats_now = ALLOCATION_STATS.with(|stats| stats.borrow().clone());
        AllocationStats {
            count_alloc: stats_now.count_alloc - stats_old.count_alloc,
            bytes_alloc: stats_now.bytes_alloc - stats_old.bytes_alloc,
            count_dealloc: stats_now.count_dealloc - stats_old.count_dealloc,
            bytes_dealloc: stats_now.bytes_dealloc - stats_old.bytes_dealloc,
        }
    }

    fn track_allocation(&mut self, bytes_alloc: usize) {
        self.count_alloc += 1;
        self.bytes_alloc += bytes_alloc;
    }

    fn track_deallocation(&mut self, bytes_dealloc: usize) {
        self.count_dealloc += 1;
        self.bytes_dealloc += bytes_dealloc;
    }
}

unsafe impl std::alloc::GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let allocated = unsafe { std::alloc::System.alloc(layout) };
        if !allocated.is_null() {
            ALLOCATION_STATS.with(|stats| stats.borrow_mut().track_allocation(layout.size()));
        }
        allocated
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        unsafe {
            std::alloc::System.dealloc(ptr, layout);
        }
        ALLOCATION_STATS.with(|stats| stats.borrow_mut().track_deallocation(layout.size()));
    }
}

#[test]
fn payload_independent_allocations() -> Result<()> {
    let batch_single = measure_allocations(1)?;
    let batch_full = measure_allocations(8189)?;

    assert_eq!(batch_single.count_alloc, batch_full.count_alloc);
    assert_eq!(batch_single.bytes_alloc, batch_full.bytes_alloc);

    Ok(())
}

#[test]
fn zero_allocation_usage() -> Result<()> {
    let (client, _guard) = test_client()?;

    let accounts = vec![tb::Account {
        id: tb::id(),
        user_data_128: tb::id(),
        ledger: TEST_LEDGER,
        code: TEST_CODE,
        ..Default::default()
    }];
    let mut lookup_ids = Vec::with_capacity(1);
    let lookup_results = Vec::with_capacity(1);
    let account_results = Vec::with_capacity(1);

    // check that no reallocation took place
    let lookup_buffer = (lookup_results.as_ptr(), lookup_results.capacity());
    let account_buffer = (account_results.as_ptr(), account_results.capacity());

    let completion = tb::Completion::new();

    block_on(async {
        let alloc_stats = AllocationStats::snapshot();

        let (completion, accounts, account_results) = client
            .create_accounts_reusable(completion, accounts, account_results)
            .await?;
        assert_eq!(account_results.len(), 1);
        assert_eq!(account_results.as_ptr(), account_buffer.0);
        assert_eq!(account_results.capacity(), account_buffer.1);

        lookup_ids.push(accounts[0].id);
        let (completion, ids, mut lookup_results) = client
            .lookup_accounts_reusable(completion, lookup_ids, lookup_results)
            .await?;
        assert_eq!(lookup_results.len(), 1);
        assert_eq!(lookup_results[0].id, accounts[0].id);
        assert_eq!(lookup_results.as_ptr(), lookup_buffer.0);
        assert_eq!(lookup_results.capacity(), lookup_buffer.1);

        // also test filters on same completion to check different representation size in op state
        let filter = tb::QueryFilter {
            user_data_128: accounts[0].user_data_128,
            ledger: TEST_LEDGER,
            limit: 1,
            ..Default::default()
        };

        lookup_results.clear();
        let (completion, filter, mut lookup_results) = client
            .query_accounts_reusable(completion, filter, lookup_results)
            .await?;
        assert_eq!(lookup_results.len(), 1);
        assert_eq!(lookup_results[0].user_data_128, filter.user_data_128);
        assert_eq!(lookup_results.as_ptr(), lookup_buffer.0);
        assert_eq!(lookup_results.capacity(), lookup_buffer.1);

        lookup_results.clear();
        let (_completion, _ids, lookup_results) = client
            .lookup_accounts_reusable(completion, ids, lookup_results)
            .await?;
        assert_eq!(lookup_results[0].id, accounts[0].id);
        assert_eq!(lookup_results.as_ptr(), lookup_buffer.0);
        assert_eq!(lookup_results.capacity(), lookup_buffer.1);

        assert_eq!(alloc_stats.diff().bytes_alloc, 0);
        assert_eq!(alloc_stats.diff().count_alloc, 0);

        Ok(())
    })
}

#[test]
fn dropping_completion_releases_state() {
    let stats = AllocationStats::snapshot();
    let completion = tb::Completion::new();
    drop(completion);
    let diff = stats.diff();
    assert_eq!(diff.count_alloc, 1);
    // we can only assert this on the main thread, since allocation stats are thread local.
    assert_eq!(diff.count_alloc, diff.count_dealloc);
    assert_eq!(diff.bytes_alloc, diff.bytes_dealloc);
}

fn measure_allocations(batch_size: usize) -> Result<AllocationStats> {
    let accounts: Vec<_> = (0..batch_size)
        .map(|_| tb::Account {
            id: tb::id(),
            ledger: TEST_LEDGER,
            code: TEST_CODE,
            ..Default::default()
        })
        .collect();
    let results = Vec::with_capacity(batch_size);
    let result_buffer = (results.as_ptr(), results.capacity());

    block_on(async move {
        let (client, _guard) = test_client()?;
        let alloc_stats = AllocationStats::snapshot();

        let completion = tb::Completion::new();
        let (_completion, accounts, results) = client
            .create_accounts_reusable(completion, accounts, results)
            .await?;

        assert_eq!(accounts.len(), batch_size);
        assert_eq!(results.len(), batch_size);
        assert_eq!(results.as_ptr(), result_buffer.0);
        assert_eq!(results.capacity(), result_buffer.1);
        assert!(results.iter().all(|result| {
            result.timestamp > 0 && result.status == tb::CreateAccountStatus::Created
        }));

        Ok(alloc_stats.diff())
    })
}
