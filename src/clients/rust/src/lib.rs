//! The official TigerBeetle client for Rust.
//!
//! This is a client library for the [TigerBeetle] financial database.
//! To use, create a [`Client`] and call its methods to make requests.
//!
//! The client presents an async interface, but does not depend on a specific
//! Rust async runtime. Instead it contains its own off-thread event loop,
//! shared by all official TigerBeetle clients. Thus it should integrate
//! seamlessly into any Rust codebase.
//!
//! The cost of this is that it does link to a non-Rust static library (called `tb_client`),
//! and it does need to context switch between threads for every request.
//! The native linking should be handled seamlessly on all supported platforms,
//! and the context switching overhead is expected to be low compared to the cost of
//! networking and disk I/O.
//!
//! [TigerBeetle]: https://tigerbeetle.com
//!
//!
//! # Example
//!
//! ```no_run
//! use tigerbeetle as tb;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Connect to TigerBeetle
//! let client = tb::Client::new(0, "127.0.0.1:3000")?;
//!
//! // Create accounts. Using TigerBeetle IDs is recommended.
//! let account_id1 = tb::id();
//! let account_id2 = tb::id();
//!
//! let accounts = [
//!     tb::Account {
//!         id: account_id1,
//!         ledger: 1,
//!         code: 1,
//!         flags: tb::AccountFlags::History,
//!         ..Default::default()
//!     },
//!     tb::Account {
//!         id: account_id2,
//!         ledger: 1,
//!         code: 1,
//!         flags: tb::AccountFlags::History,
//!         ..Default::default()
//!     },
//! ];
//!
//! let account_results = client.create_accounts(&accounts).await?;
//!
//! // A successful reply contains one result code for each account.
//! assert_eq!(account_results.len(), 2);
//!
//! // Create a transfer between accounts
//! let transfer_id = tb::id();
//! let transfers = [tb::Transfer {
//!     id: transfer_id,
//!     debit_account_id: account_id1,
//!     credit_account_id: account_id2,
//!     amount: 100,
//!     ledger: 1,
//!     code: 1,
//!     ..Default::default()
//! }];
//!
//! let transfer_results = client.create_transfers(&transfers).await?;
//! assert_eq!(transfer_results.len(), 1);
//!
//! // Look up the accounts to see the transfer result.
//! let accounts = client.lookup_accounts(&[account_id1, account_id2]).await?;
//! let account1 = accounts[0];
//! let account2 = accounts[1];
//!
//! assert_eq!(account1.id, account_id1);
//! assert_eq!(account2.id, account_id2);
//! assert_eq!(account1.debits_posted, 100);
//! assert_eq!(account2.credits_posted, 100);
//! # Ok(())
//! # }
//! ```
//!
//! # Request batching
//!
//! Most transaction and query operations support multiple events of the same
//! type at once (this can be seen in the request method signatures accepting
//! slices of their input types) and it is strongly recommended to submit many
//! events in a single request at once as TigerBeetle will only reach its
//! performance limits when events are received in large batches. The client
//! _does_ implement its own internal batching and will attempt to create them
//! efficiently, but it can be more efficient for applications to create their own
//! batches based on understanding of their own architectural needs and limitations.
//!
//! In TigerBeetle's standard build-time configuration **the maximum number of
//! events per batch is 8189**. If the events in a request exceed this number
//! its future will return [`PacketError::TooMuchData`].
//!
//!
//! # Memory Management
//!
//! The convenience methods on [`Client`] copy their input slice and allocate an output vector.
//! Applications can instead use methods such as [`Client::create_accounts_reusable`] to manage
//! memory explicitly, passing a [`Completion`], an owned input vector or filter, and an output
//! vector. Results are appended to the output vector. The client never allocates in this case.
//!
//! On success, the completion, input, and output are returned for reuse. A completion is untyped
//! and is reusable across different operations, including queries. Allocate the completion state
//! ahead of time, reuse it, and ensure output vectors are large enough to avoid heap allocations.
//!
//! ```no_run
//! # use tigerbeetle as tb;
//! # async fn example(client: &tb::Client, accounts: Vec<tb::Account>) -> Result<(), tb::PacketError> {
//! let completion = tb::Completion::new();
//! let results = Vec::with_capacity(accounts.len());
//! let (completion, mut accounts, mut results) = client
//!     .create_accounts_reusable(completion, accounts, results)
//!     .await?;
//! // Inspect results here...
//!
//! accounts.clear();
//! results.clear();
//! // Refill `accounts` with the next batch here.
//! let (_completion, _accounts, _results) = client
//!     .create_accounts_reusable(completion, accounts, results)
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! Each reusable method returns `Result<(Completion, Source, Vec<OutputItem>), PacketError>`,
//! where `Source` is an input vector or a single query filter. On [`PacketError`], all three
//! are dropped. Requests are queued before the method returns and may thus be executed, even
//! if the returned future is never polled; dropping the future does not cancel the operation.
//!
//!
//! # Range query limits
//!
//! TigerBeetle's range queries, [`get_account_transfers`], [`get_account_balances`],
//! [`query_accounts`] and [`query_transfers`], also have a limit to how many results they return.
//!
//! In TigerBeetle's standard build-time configuration **the maximum number of
//! results returned is 8189**.
//!
//! If the server returns a full batch for a range query, then further results
//! can be paged by incrementing `timeout_max` to one greater than the highest
//! timeout returned in the previous batch, and issuing a new query with
//! otherwise the same filter. This process can be repeated until the server
//! returns a partial batch.
//!
//! [`get_account_transfers`]: Client::get_account_transfers
//! [`get_account_balances`]: Client::get_account_balances
//! [`query_accounts`]: Client::query_accounts
//! [`query_transfers`]: Client::query_transfers
//!
//! Here is an example of paging to get started with:
//!
//! ```no_run
//! use tigerbeetle as tb;
//! use futures::{stream, Stream};
//!
//! fn get_account_transfers_paged(
//!     client: &tb::Client,
//!     event: tb::AccountFilter,
//! ) -> impl Stream<Item = std::result::Result<Vec<tb::Transfer>, tb::PacketError>> + '_ {
//!     assert!(
//!         event.limit > 1,
//!         "paged queries should use an explicit limit"
//!     );
//!
//!     enum State {
//!         Start,
//!         Continue(u64),
//!         End,
//!     }
//!
//!     let is_reverse = (event.flags.0 & tb::AccountFilterFlags::Reversed.0) != 0;
//!
//!     futures::stream::unfold(State::Start, move |state| async move {
//!         let event = match state {
//!             State::Start => event,
//!             State::Continue(timestamp_begin) => {
//!                 if !is_reverse {
//!                     tb::AccountFilter {
//!                         timestamp_min: timestamp_begin,
//!                         ..event
//!                     }
//!                 } else {
//!                     tb::AccountFilter {
//!                         timestamp_max: timestamp_begin,
//!                         ..event
//!                     }
//!                 }
//!             }
//!             State::End => return None,
//!         };
//!         let result = client
//!             .get_account_transfers(event)
//!             .await;
//!         match result {
//!             Ok(result_next) => {
//!                 let result_len = u32::try_from(result_next.len()).expect("u32");
//!                 let must_page = result_len == event.limit;
//!                 if must_page {
//!                     let timestamp_first = result_next.first().expect("item").timestamp;
//!                     let timestamp_last = result_next.last().expect("item").timestamp;
//!                     let (timestamp_begin_next, should_continue) = if !is_reverse {
//!                         assert!(timestamp_first < timestamp_last);
//!                         let timestamp_begin_next = timestamp_last.checked_add(1).expect("overflow");
//!                         assert_ne!(timestamp_begin_next, u64::MAX);
//!                         let should_continue =
//!                             timestamp_begin_next <= event.timestamp_max || event.timestamp_max == 0;
//!                         (timestamp_begin_next, should_continue)
//!                     } else {
//!                         assert!(timestamp_first > timestamp_last);
//!                         let timestamp_begin_next = timestamp_last.checked_sub(1).expect("overflow");
//!                         assert_ne!(timestamp_begin_next, 0);
//!                         let should_continue =
//!                             timestamp_begin_next >= event.timestamp_min || event.timestamp_min == 0;
//!                         (timestamp_begin_next, should_continue)
//!                     };
//!                     if should_continue {
//!                         Some((Ok(result_next), State::Continue(timestamp_begin_next)))
//!                     } else {
//!                         Some((Ok(result_next), State::End))
//!                     }
//!                 } else {
//!                     Some((Ok(result_next), State::End))
//!                 }
//!             }
//!             Err(error) => Some((Err(error), State::End)),
//!         }
//!     })
//! }
//! ```
//!
//!
//! # Response futures and client lifetime considerations
//!
//! Responses to requests implement [`Future`]. It is not strictly necessary for
//! applications to `await` these futures &mdash; requests are enqueued as soon as
//! the request method is called and will be executed even if the future is dropped.
//!
//! It is possible to drop a `Client` while request futures are still outstanding.
//! In this case any pending requests will be completed with [`PacketError::ClientClosed`].
//! Request futures may resolve to successful results even after the client is closed.
//!
//! When `Client` is dropped without calling [`close`], it will shutdown correctly,
//! but some of that work happens off-thread after the drop completes.
//!
//! For orderly shutdown, it is recommended to await all request futures prior to
//! destroying the client, and to destroy the client by calling `close` and awaiting
//! its return value.
//!
//! [`close`]: Client::close
//!
//!
//! # Concurrency and multithreading
//!
//! Multiple requests may be submitted concurrently from a single client.
//! The server only supports one in-flight request per client though, so the client
//! will internally buffer concurrent requests. To truly have multiple requests in
//! flight concurrently, multiple clients can be created, though note that there is
//! a hard-coded limit on how many clients can be connected to the server simultaneously.
//!
//! The `Client` type implements `Send` and `Sync` and may be used in parallel
//! across multiple threads or async tasks, e.g. by placing it into an [`Arc`].
//! This can be useful because it allows the client to leverage its internal request
//! batching to batch events from multiple threads (or tasks), which can provide a
//! performance advantage if your application doesn't naturally create large batches.
//!
//!
//! [`Arc`]: `std::sync::Arc`
//!
//!
//! # TigerBeetle time-based identifiers
//!
//! Accounts and transfers must have globally unique identifiers. The generation
//! of these is application-specific, and any scheme that guarantees unique IDs
//! will work. Barring other constraints, TigerBeetle highly recommends using
//! [TigerBeetle time-based identifiers][tbid]. This crate provides an
//! implementation in the [`id`] function. For performance reasons, we recommend using
//! [TigerBeetle time-based identifiers][tbid] or other increasing, non-random identifiers.
//!
//! For additional considerations when choosing an ID scheme
//! see [the TigerBeetle documentation on data modeling][tbdataid].
//!
//! [tbid]: https://docs.tigerbeetle.com/coding/data-modeling/#tigerbeetle-time-based-identifiers-recommended
//! [tbdataid]: https://docs.tigerbeetle.com/coding/data-modeling/#id
//!
//!
//! # Use in non-async codebases
//!
//! The TigerBeetle client is async-only, but if you're working in a synchronous
//! codebase, you can use something like [`futures::executor::block_on`] to run async operations
//! to completion.
//!
//! [`futures::executor::block_on`]: https://docs.rs/futures/latest/futures/executor/fn.block_on.html
//!
//! ```no_run
//! use futures::executor::block_on;
//! use tigerbeetle as tb;
//!
//! fn synchronous_function() -> Result<(), Box<dyn std::error::Error>> {
//!     block_on(async {
//!         let client = tb::Client::new(0, "127.0.0.1:3000")?;
//!
//!         let accounts = [tb::Account {
//!             id: tb::id(),
//!             ledger: 1,
//!             code: 1,
//!             ..Default::default()
//!         }];
//!
//!         let results = client.create_accounts(&accounts).await?;
//!
//!         Ok(())
//!     })
//! }
//! ```
//!
//! Note that `block_on` will block the current thread until the async operation
//! completes, so this approach works best for simple use cases or when you need
//! to integrate TigerBeetle into an existing synchronous application.
//!
//!
//! # Rust structure binary representation and the TigerBeetle protocol
//!
//! Most types in this library are ABI-compatible with the underlying protocol
//! definition and can be cast (unsafely) directly to and from byte buffers
//! on all supported platforms, though this should not be required for typical
//! application purposes. Protocol-compatible types are defined within the
//! `tb_client` module; types which are not ABI-compatible are defined separately.
//!
//!
//! # References
//!
//! [The TigerBeetle Reference](https://docs.tigerbeetle.com/reference/).
mod oneshot;

// The generated bindings.
// These are not part of the public API but are re-exported hidden
// so that the vortex driver can parse the TB protocol directly.
#[allow(unused)]
#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[rustfmt::skip]
#[doc(hidden)]
pub mod tb_client;

use tb_client as tbc;

mod conversions;
mod op;
mod time_based_id;

use std::future::Future;
use std::os::raw::c_char;

pub use time_based_id::id;

/// The TigerBeetle client.
pub struct Client {
    client: *mut tbc::tb_client_t,
}

unsafe impl Send for Client {}
unsafe impl Sync for Client {}

impl Client {
    /// Create a new TigerBeetle client.
    ///
    /// # Addresses
    ///
    /// The `addresses` argument is a comma-separated string of addresses, where
    /// each may be either an IP4 address, a port number, or the pair of IP4
    /// address and port number separated by a colon. Examples include
    /// `127.0.0.1`, `3001`, `127.0.0.1:3001` and
    /// `127.0.0.1,3002,127.0.0.1:3003`. The default IP address is `127.0.0.1`
    /// and default port is `3001`.
    ///
    /// This is the same address format supported by the TigerBeetle CLI.
    ///
    /// # References
    ///
    /// [Client Sessions](https://docs.tigerbeetle.com/reference/sessions/).
    pub fn new(cluster_id: u128, addresses: &str) -> Result<Client, InitError> {
        unsafe {
            let mut tb_client = Box::new(tbc::tb_client_t {
                opaque: Default::default(),
            });
            let status = tbc::tb_client_init(
                &mut *tb_client,
                &cluster_id.to_le_bytes(),
                addresses.as_ptr() as *const c_char,
                addresses.len().try_into().expect("too many addresses"),
                op::COMPLETION_CONTEXT,
                Some(op::on_completion),
            );
            if status == tbc::TB_INIT_STATUS_TB_INIT_SUCCESS {
                Ok(Client {
                    client: Box::into_raw(tb_client),
                })
            } else {
                Err(status.into())
            }
        }
    }

    /// Create one or more accounts.
    ///
    /// Accounts to create are provided as a slice of input [`Account`] events. Their fields must
    /// be initialized as described in the corresponding [protocol reference](#protocol-reference).
    ///
    /// The request is queued for submission prior to return of this function;
    /// dropping the returned [`Future`] will not cancel the request.
    ///
    /// # Interpreting the return value
    ///
    /// If the operation returns a [`PacketError`], you can assume that none of the events
    /// were processed.
    ///
    /// The results of events are represented individually. There are two
    /// related event result types: `CreateAccountStatus` is the enum of
    /// possible outcomes, and `CreateAccountResult` which includes both the
    /// `status` enum and the `timestamp` when the event was processed.
    ///
    /// Note that a status of `CreateAccountStatus::Exists` should often be treated
    /// the same as `CreateAccountStatus::Created`, as it also returns the same `timestamp`
    /// of the original account. This result can happen in cases of application crashes
    /// or other scenarios where requests have been replayed.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use tigerbeetle as tb;
    ///
    /// async fn make_create_accounts_request(
    ///     client: &tb::Client,
    ///     accounts: &[tb::Account],
    /// ) -> std::result::Result<(), Box<dyn std::error::Error>> {
    ///     let account_results = client.create_accounts(accounts).await?;
    ///     assert_eq!(accounts.len(), account_results.len());
    ///     let it = accounts
    ///         .iter()
    ///         .enumerate()
    ///         .map(move |(i, account)| (account, account_results[i]));
    ///
    ///     for (account, account_result) in it {
    ///         match account_result.status {
    ///             tb::CreateAccountStatus::Created | tb::CreateAccountStatus::Exists => {
    ///                 handle_create_account_success(account, account_result).await?;
    ///             }
    ///             _ => {
    ///                 handle_create_account_failure(account, account_result).await?;
    ///             }
    ///         }
    ///     }
    ///     Ok(())
    /// }
    ///
    /// async fn handle_create_account_success(
    ///     _account: &tb::Account,
    ///     _result: tb::CreateAccountResult,
    /// ) -> Result<(), Box<dyn std::error::Error>> {
    ///     Ok(())
    /// }
    ///
    /// async fn handle_create_account_failure(
    ///     _account: &tb::Account,
    ///     _result: tb::CreateAccountResult,
    /// ) -> Result<(), Box<dyn std::error::Error>> {
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Maximum batch size
    ///
    /// If the length of the `events` argument exceeds the maximum batch size, the future returns
    /// [`PacketError::TooMuchData`]. In TigerBeetle's standard
    /// build-time configuration the maximum batch size is 8189.
    ///
    /// # Protocol reference
    ///
    /// [`create_accounts`](https://docs.tigerbeetle.com/reference/requests/create_accounts).
    pub fn create_accounts(
        &self,
        events: &[Account],
    ) -> impl Future<Output = Result<Vec<CreateAccountResult>, PacketError>> {
        self.execute_allocating::<tbc::CreateAccounts>(events.to_vec())
    }

    /// Create one or more transfers.
    ///
    /// Transfers to create are provided as a slice of input [`Transfer`] events. Their fields must
    /// be initialized as described in the corresponding [protocol reference](#protocol-reference).
    ///
    /// The request is queued for submission prior to return of this function;
    /// dropping the returned [`Future`] will not cancel the request.
    ///
    /// # Interpreting the return value
    ///
    /// If the operation returns a [`PacketError`], you can assume that none of the events
    /// were processed.
    ///
    /// The results of events are represented individually. There are two related event result
    /// types: `CreateTransferStatus`, the enum of possible outcomes, and
    /// [`CreateTransferResult`], which includes both the `status` and the `timestamp` when the
    /// event was processed.
    ///
    /// A status of `CreateTransferStatus::Exists` should often be treated the same as
    /// `CreateTransferStatus::Created`, as it also returns the original transfer's `timestamp`.
    /// This can happen after an application crash or in other scenarios where requests are
    /// replayed.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use tigerbeetle as tb;
    ///
    /// async fn make_create_transfers_request(
    ///     client: &tb::Client,
    ///     transfers: &[tb::Transfer],
    /// ) -> std::result::Result<(), Box<dyn std::error::Error>> {
    ///     let transfer_results = client.create_transfers(transfers).await?;
    ///     let it = transfers
    ///         .iter()
    ///         .enumerate()
    ///         .map(move |(i, transfer)| (transfer, transfer_results[i]));
    ///     for (transfer, transfer_result) in it {
    ///         match transfer_result.status {
    ///             tb::CreateTransferStatus::Created | tb::CreateTransferStatus::Exists => {
    ///                 handle_create_transfer_success(transfer, transfer_result).await?;
    ///             }
    ///             _ => {
    ///                 handle_create_transfer_failure(transfer, transfer_result).await?;
    ///             }
    ///         }
    ///     }
    ///     Ok(())
    /// }
    ///
    /// # async fn handle_create_transfer_success(
    /// #     _transfer: &tb::Transfer,
    /// #     _result: tb::CreateTransferResult,
    /// # ) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
    /// # async fn handle_create_transfer_failure(
    /// #     _transfer: &tb::Transfer,
    /// #     _result: tb::CreateTransferResult,
    /// # ) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
    /// ```
    ///
    /// # Maximum batch size
    ///
    /// If the number of events exceeds the maximum batch size, the future returns
    /// [`PacketError::TooMuchData`]. In TigerBeetle's standard
    /// build-time configuration the maximum batch size is 8189.
    ///
    /// # Protocol reference
    ///
    /// [`create_transfers`](https://docs.tigerbeetle.com/reference/requests/create_transfers).
    pub fn create_transfers(
        &self,
        events: &[Transfer],
    ) -> impl Future<Output = Result<Vec<CreateTransferResult>, PacketError>> {
        self.execute_allocating::<tbc::CreateTransfers>(events.to_vec())
    }

    /// Query individual accounts by ID.
    ///
    /// Account IDs are provided as a slice. The request is queued before this function returns;
    /// dropping the returned [`Future`] does not cancel it.
    ///
    /// # Interpreting the return value
    ///
    /// If the operation returns a [`PacketError`], you can assume that none of the events
    /// were processed.
    ///
    /// On success, the output contains found accounts in request order, but omits IDs that were not
    /// found. Compare the returned IDs with the input IDs to identify missing accounts, as shown
    /// below.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use tigerbeetle as tb;
    ///
    /// async fn make_lookup_accounts_request(
    ///     client: &tb::Client,
    ///     accounts: &[u128],
    /// ) -> std::result::Result<(), Box<dyn std::error::Error>> {
    ///     let lookup_accounts_results = client.lookup_accounts(accounts).await?;
    ///     let lookup_accounts_results_merged =
    ///         merge_lookup_accounts_results(&accounts, lookup_accounts_results);
    ///     for (account_id, maybe_account) in lookup_accounts_results_merged {
    ///         match maybe_account {
    ///             Some(account) => {
    ///                 handle_lookup_accounts_success(account).await?;
    ///             }
    ///             None => {
    ///                 handle_lookup_accounts_failure(account_id).await?;
    ///             }
    ///         }
    ///     }
    ///     Ok(())
    /// }
    ///
    /// /// An iterator over both successful and unsuccessful lookup results.
    /// fn merge_lookup_accounts_results(
    ///     accounts: &[u128],
    ///     results: Vec<tb::Account>,
    /// ) -> impl Iterator<Item = (u128, Option<tb::Account>)> + '_ {
    ///     let mut results = results.into_iter().peekable();
    ///     accounts.iter().map(move |&id| match results.peek() {
    ///         Some(acc) if acc.id == id => (id, results.next()),
    ///         _ => (id, None),
    ///     })
    /// }
    ///
    /// # async fn handle_lookup_accounts_success(
    /// #     _account: tb::Account,
    /// # ) -> std::result::Result<(), Box<dyn std::error::Error>> { Ok(()) }
    /// # async fn handle_lookup_accounts_failure(
    /// #     _account_id: u128,
    /// # ) -> std::result::Result<(), Box<dyn std::error::Error>> { Ok(()) }
    /// ```
    ///
    /// # Maximum batch size
    ///
    /// If the number of IDs exceeds the maximum batch size, the future returns
    /// [`PacketError::TooMuchData`]. In TigerBeetle's standard
    /// build-time configuration the maximum batch size is 8189.
    ///
    /// # Protocol reference
    ///
    /// [`lookup_accounts`](https://docs.tigerbeetle.com/reference/requests/lookup_accounts).
    pub fn lookup_accounts(
        &self,
        events: &[u128],
    ) -> impl Future<Output = Result<Vec<Account>, PacketError>> {
        self.execute_allocating::<tbc::LookupAccounts>(events.to_vec())
    }

    /// Query individual transfers by ID.
    ///
    /// Transfer IDs are provided as a slice. The request is queued before this function returns;
    /// dropping the returned [`Future`] does not cancel it.
    ///
    /// # Interpreting the return value
    ///
    /// If the operation returns a [`PacketError`], you can assume that none of the events
    /// were processed.
    ///
    /// On success, the output contains found transfers in request order, but omits IDs that were
    /// not found. Compare the returned IDs with the input IDs to identify missing transfers, as
    /// shown below.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use tigerbeetle as tb;
    ///
    /// async fn make_lookup_transfers_request(
    ///     client: &tb::Client,
    ///     transfers: &[u128],
    /// ) -> std::result::Result<(), Box<dyn std::error::Error>> {
    ///     let lookup_transfers_results = client.lookup_transfers(transfers).await?;
    ///     let lookup_transfers_results_merged =
    ///         merge_lookup_transfers_results(&transfers, lookup_transfers_results);
    ///     for (transfer_id, maybe_transfer) in lookup_transfers_results_merged {
    ///         match maybe_transfer {
    ///             Some(transfer) => {
    ///                 handle_lookup_transfers_success(transfer).await?;
    ///             }
    ///             None => {
    ///                 handle_lookup_transfers_failure(transfer_id).await?;
    ///             }
    ///         }
    ///     }
    ///     Ok(())
    /// }
    ///
    /// /// An iterator over both successful and unsuccessful lookup results.
    /// fn merge_lookup_transfers_results(
    ///     transfers: &[u128],
    ///     results: Vec<tb::Transfer>,
    /// ) -> impl Iterator<Item = (u128, Option<tb::Transfer>)> + '_ {
    ///     let mut results = results.into_iter().peekable();
    ///     transfers.iter().map(move |&id| match results.peek() {
    ///         Some(transfer) if transfer.id == id => (id, results.next()),
    ///         _ => (id, None),
    ///     })
    /// }
    ///
    /// # async fn handle_lookup_transfers_success(
    /// #     _transfer: tb::Transfer,
    /// # ) -> std::result::Result<(), Box<dyn std::error::Error>> { Ok(()) }
    /// # async fn handle_lookup_transfers_failure(
    /// #     _transfer_id: u128,
    /// # ) -> std::result::Result<(), Box<dyn std::error::Error>> { Ok(()) }
    /// ```
    ///
    /// # Maximum batch size
    ///
    /// If the number of IDs exceeds the maximum batch size, the future returns
    /// [`PacketError::TooMuchData`]. In TigerBeetle's standard
    /// build-time configuration the maximum batch size is 8189.
    ///
    /// # Protocol reference
    ///
    /// [`lookup_transfers`](https://docs.tigerbeetle.com/reference/requests/lookup_transfers).
    pub fn lookup_transfers(
        &self,
        events: &[u128],
    ) -> impl Future<Output = Result<Vec<Transfer>, PacketError>> {
        self.execute_allocating::<tbc::LookupTransfers>(events.to_vec())
    }

    /// Query multiple transfers for a single account.
    ///
    /// The passed [`AccountFilter`] selects the account, transfer types, timestamp range, result
    /// order, and limit as described in the [protocol reference](#protocol-reference).
    ///
    /// The request is queued before this function returns; dropping the returned [`Future`] does
    /// not cancel it.
    ///
    /// # Interpreting the return value
    ///
    /// If the operation returns a [`PacketError`], you can assume that none of the events
    /// were processed.
    ///
    /// On success, it returns the matching transfers.
    ///
    /// # Protocol reference
    ///
    /// [`get_account_transfers`](https://docs.tigerbeetle.com/reference/requests/get_account_transfers).
    pub fn get_account_transfers(
        &self,
        filter: AccountFilter,
    ) -> impl Future<Output = Result<Vec<Transfer>, PacketError>> {
        self.execute_allocating::<tbc::GetAccountTransfers>(filter)
    }

    /// Query historical account balances for a single account.
    ///
    /// The passed [`AccountFilter`] selects the account, timestamp range, result order, and limit
    /// as described in the [protocol reference](#protocol-reference).
    ///
    /// The request is queued before this function returns; dropping the returned [`Future`] does
    /// not cancel it.
    ///
    /// # Interpreting the return value
    ///
    /// If the operation returns a [`PacketError`], you can assume that none of the events
    /// were processed.
    ///
    /// On success, it returns the matching balances.
    ///
    /// # Protocol reference
    ///
    /// [`get_account_balances`](https://docs.tigerbeetle.com/reference/requests/get_account_balances).
    pub fn get_account_balances(
        &self,
        filter: AccountFilter,
    ) -> impl Future<Output = Result<Vec<AccountBalance>, PacketError>> {
        self.execute_allocating::<tbc::GetAccountBalances>(filter)
    }

    /// Query multiple accounts related by fields and timestamps.
    ///
    /// The passed [`QueryFilter`] specifies the fields, timestamp range, result order, and limit as
    /// described in the [protocol reference](#protocol-reference).
    ///
    /// The request is queued before this function returns; dropping the returned [`Future`] does
    /// not cancel it.
    ///
    /// # Interpreting the return value
    ///
    /// If the operation returns a [`PacketError`], you can assume that none of the events
    /// were processed.
    ///
    /// On success, it returns the matching accounts.
    ///
    /// # Protocol reference
    ///
    /// [`query_accounts`](https://docs.tigerbeetle.com/reference/requests/query_accounts).
    pub fn query_accounts(
        &self,
        filter: QueryFilter,
    ) -> impl Future<Output = Result<Vec<Account>, PacketError>> {
        self.execute_allocating::<tbc::QueryAccounts>(filter)
    }

    /// Query multiple transfers related by fields and timestamps.
    ///
    /// The passed [`QueryFilter`] specifies the fields, timestamp range, result order, and limit as
    /// described in the [protocol reference](#protocol-reference).
    ///
    /// The request is queued before this function returns; dropping the returned [`Future`] does
    /// not cancel it.
    ///
    /// # Interpreting the return value
    ///
    /// If the operation returns a [`PacketError`], you can assume that none of the events
    /// were processed.
    ///
    /// On success, it returns the matching transfers.
    ///
    /// # Protocol reference
    ///
    /// [`query_transfers`](https://docs.tigerbeetle.com/reference/requests/query_transfers).
    pub fn query_transfers(
        &self,
        filter: QueryFilter,
    ) -> impl Future<Output = Result<Vec<Transfer>, PacketError>> {
        self.execute_allocating::<tbc::QueryTransfers>(filter)
    }

    /// Create accounts using reusable completion state and owned buffers.
    ///
    /// See [`Self::create_accounts`] for requirements, result interpretation, and limits.
    /// Results are appended to `results`. With enough spare capacity, no heap allocation
    /// is needed. Returns `(completion, accounts, results)` on success, drops all three
    /// on error. Clear results before reuse to replace previous results.
    ///
    /// The request is queued before return; dropping the future does not cancel it.
    pub fn create_accounts_reusable(
        &self,
        completion: Completion,
        accounts: Vec<Account>,
        results: Vec<CreateAccountResult>,
    ) -> impl Future<Output = Result<(Completion, Vec<Account>, Vec<CreateAccountResult>), PacketError>>
    {
        self.execute::<tbc::CreateAccounts>(completion, accounts, results)
    }

    /// Create transfers using reusable completion state and owned buffers.
    ///
    /// See [`Self::create_transfers`] for requirements, result interpretation, and limits.
    /// Results are appended to `results`. With sufficient spare capacity, no heap allocation
    /// is needed. Returns `(completion, transfers, results)` on success, drops all three on error.
    /// Clear results before reuse to replace previous results.
    ///
    /// The request is queued before return; dropping the future does not cancel it.
    pub fn create_transfers_reusable(
        &self,
        completion: Completion,
        transfers: Vec<Transfer>,
        results: Vec<CreateTransferResult>,
    ) -> impl Future<Output = Result<(Completion, Vec<Transfer>, Vec<CreateTransferResult>), PacketError>>
    {
        self.execute::<tbc::CreateTransfers>(completion, transfers, results)
    }

    /// Look up accounts using reusable completion state and owned buffers.
    ///
    /// See [`Self::lookup_accounts`] for result ordering, missing IDs, and limits.
    /// Results are appended to `results`. With sufficient spare capacity, no heap allocation
    /// is needed. Returns `(completion, ids, results)` on success, drops all three on error.
    /// Clear results before reuse to replace previous results.
    ///
    /// The request is queued before return; dropping the future does not cancel it.
    pub fn lookup_accounts_reusable(
        &self,
        completion: Completion,
        ids: Vec<u128>,
        results: Vec<Account>,
    ) -> impl Future<Output = Result<(Completion, Vec<u128>, Vec<Account>), PacketError>> {
        self.execute::<tbc::LookupAccounts>(completion, ids, results)
    }

    /// Look up transfers using reusable completion state and owned buffers.
    ///
    /// See [`Self::lookup_transfers`] for result ordering, missing IDs, and limits.
    /// Results are appended to `results`. With sufficient spare capacity, no heap allocation
    /// is needed. Returns `(completion, ids, results)` on success, drops all three on error.
    /// Clear results before reuse to replace previous results.
    ///
    /// The request is queued before return; dropping the future does not cancel it.
    pub fn lookup_transfers_reusable(
        &self,
        completion: Completion,
        ids: Vec<u128>,
        results: Vec<Transfer>,
    ) -> impl Future<Output = Result<(Completion, Vec<u128>, Vec<Transfer>), PacketError>> {
        self.execute::<tbc::LookupTransfers>(completion, ids, results)
    }

    /// Query account transfers using reusable completion state and an owned output buffer.
    ///
    /// See [`Self::get_account_transfers`] for filter semantics and limits.
    /// Results are appended to `results`. With sufficient spare capacity, no heap allocation
    /// is needed. Returns `(completion, filter, results)` on success, drops all three on error.
    /// Clear results before reuse to replace previous results.
    ///
    /// The request is queued before return; dropping the future does not cancel it.
    pub fn get_account_transfers_reusable(
        &self,
        completion: Completion,
        filter: AccountFilter,
        results: Vec<Transfer>,
    ) -> impl Future<Output = Result<(Completion, AccountFilter, Vec<Transfer>), PacketError>> {
        self.execute::<tbc::GetAccountTransfers>(completion, filter, results)
    }

    /// Query account balances using reusable completion state and an owned output buffer.
    ///
    /// See [`Self::get_account_balances`] for filter semantics and limits.
    /// Results are appended to `results`. With sufficient spare capacity, no heap allocation
    /// is needed. Returns `(completion, filter, results)` on success, drops all three on error.
    /// Clear results before reuse to replace previous results.
    ///
    /// The request is queued before return; dropping the future does not cancel it.
    pub fn get_account_balances_reusable(
        &self,
        completion: Completion,
        filter: AccountFilter,
        results: Vec<AccountBalance>,
    ) -> impl Future<Output = Result<(Completion, AccountFilter, Vec<AccountBalance>), PacketError>>
    {
        self.execute::<tbc::GetAccountBalances>(completion, filter, results)
    }

    /// Query accounts using reusable completion state and an owned output buffer.
    ///
    /// See [`Self::query_accounts`] for filter semantics and limits.
    /// Results are appended to `results`. With sufficient spare capacity, no heap allocation
    /// is needed. Returns `(completion, filter, results)` on success, drops all three on error.
    /// Clear results before reuse to replace previous results.
    ///
    /// The request is queued before return; dropping the future does not cancel it.
    pub fn query_accounts_reusable(
        &self,
        completion: Completion,
        filter: QueryFilter,
        results: Vec<Account>,
    ) -> impl Future<Output = Result<(Completion, QueryFilter, Vec<Account>), PacketError>> {
        self.execute::<tbc::QueryAccounts>(completion, filter, results)
    }

    /// Query transfers using reusable completion state and an owned output buffer.
    ///
    /// See [`Self::query_transfers`] for filter semantics and limits.
    /// Results are appended to `results`. With sufficient spare capacity, no heap allocation
    /// is needed. Returns `(completion, filter, results)` on success, drops all thre on error.
    /// Clear results before reuse to replace previous results.
    ///
    /// The request is queued before return; dropping the future does not cancel it.
    pub fn query_transfers_reusable(
        &self,
        completion: Completion,
        filter: QueryFilter,
        results: Vec<Transfer>,
    ) -> impl Future<Output = Result<(Completion, QueryFilter, Vec<Transfer>), PacketError>> {
        self.execute::<tbc::QueryTransfers>(completion, filter, results)
    }

    /// Close the client and asynchronously wait for completion.
    ///
    /// The returned future resolves to `Err(PacketError::ClientClosed)` if the client
    /// was already invalidated by eviction.
    ///
    /// Calling `close` will cancel any pending requests. This is only possible
    /// if the futures for those requests were dropped without awaiting them.
    pub fn close(mut self) -> impl Future<Output = Result<(), PacketError>> {
        struct SendClient(*mut tbc::tb_client_t);
        unsafe impl Send for SendClient {}

        let client = std::mem::replace(&mut self.client, std::ptr::null_mut());
        let client = SendClient(client);

        let (tx, rx) = oneshot::channel::<Result<(), PacketError>>();

        std::thread::spawn(move || {
            let client = client;
            let result = unsafe { Client::deinit_raw(client.0) };
            tx.send(result);
        });

        rx
    }

    unsafe fn deinit_raw(client: *mut tbc::tb_client_t) -> Result<(), PacketError> {
        // This is a blocking function, so callers should run it off-thread.
        let status = tbc::tb_client_deinit(client);
        let result = match status {
            tbc::TB_CLIENT_STATUS_TB_CLIENT_SUCCESS => Ok(()),
            tbc::TB_CLIENT_STATUS_TB_CLIENT_CLOSED => Err(PacketError::ClientClosed),
            tbc::TB_CLIENT_STATUS_TB_CLIENT_NOT_INITIALIZED => {
                unreachable!("Client interface not initialized")
            }
            _ => {
                unreachable!("unexpected status from tb_client_deinit: {}", status)
            }
        };
        drop(Box::from_raw(client));
        result
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if self.client.is_null() {
            return;
        }
        struct SendClient(*mut tbc::tb_client_t);
        unsafe impl Send for SendClient {}

        let client = std::mem::replace(&mut self.client, std::ptr::null_mut());
        let client = SendClient(client);

        std::thread::spawn(move || {
            let client = client;
            let _ = unsafe { Client::deinit_raw(client.0) };
        });
    }
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str("Client")
    }
}

/// Reusable, type-erased request completion state.
///
/// You can allocate this once, then pass it to a [`Client`] reusable method such as
/// [`Client::create_accounts_reusable`]. On success the method returns the completion,
/// source, and output vector, allowing the same state to be used for any operation.
/// On error all three are dropped.
///
/// With pre-allocated input and sufficient spare output capacity, reuse requires no rust heap
/// allocations. Results are pushed to the output vector; clear it before reuse to replace them.
/// See [Memory Management](crate#memory-management) for an example.
pub struct Completion {
    shared: std::sync::Arc<op::OpState>,
}

impl Completion {
    /// Allocate reusable completion state without attaching input or output buffers.
    pub fn new() -> Self {
        Self {
            shared: op::OpState::new(),
        }
    }
}

impl Default for Completion {
    fn default() -> Self {
        Self::new()
    }
}

/// A TigerBeetle account.
///
/// # Protocol reference
///
/// [`Account`](https://docs.tigerbeetle.com/reference/account/).
pub type Account = tbc::Account;

/// Bitflags for the `flags` field of [`Account`].
///
/// # Protocol reference
///
/// [`Account.flags`](https://docs.tigerbeetle.com/reference/account/#flags).
pub use tbc::AccountFlags;

/// A transfer between accounts.
///
/// # Protocol reference
///
/// [`Transfer`](https://docs.tigerbeetle.com/reference/transfer).
pub type Transfer = tbc::Transfer;

/// Bitflags for the `flags` field of [`Transfer`].
///
/// # Protocol reference
///
/// [`Transfer.flags`](https://docs.tigerbeetle.com/reference/transfer/#flags).
pub use tbc::TransferFlags;

/// Filter for querying transfers and historical balances.
///
/// # Protocol reference
///
/// [`AccountFilter`](https://docs.tigerbeetle.com/reference/account-filter).
pub use tbc::AccountFilter;

/// Bitflags for the `flags` field of [`AccountFilter`].
///
/// # Protocol reference
///
/// [`AccountFilter.flags`](https://docs.tigerbeetle.com/reference/account-filter/#flags).
pub use tbc::AccountFilterFlags;

/// An account balance at a point in time.
///
/// # Protocol reference
///
/// [`AccountBalance`](https://docs.tigerbeetle.com/reference/account-balance/).
pub use tbc::AccountBalance;

/// Parameters for querying accounts and transfers.
///
/// # Protocol reference
///
/// [`QueryFilter`](https://docs.tigerbeetle.com/reference/query-filter/).
pub use tbc::QueryFilter;

/// Bitflags for the `flags` field of [`QueryFilter`].
///
/// # Protocol reference
///
/// [`QueryFilter.flags`](https://docs.tigerbeetle.com/reference/query-filter/#flags).
pub use tbc::QueryFilterFlags;

/// The result of a single [`create_accounts`] event.
///
/// For the meaning of individual enum variants see the linked protocol reference.
///
/// See also [`CreateAccountResult`], the type directly returned by `create_accounts`,
/// which contains the timestamp at which the account was created. Note that a status of
/// `CreateAccountStatus::Exists` should often be treated like `CreateAccountStatus::Created`,
/// as it also returns the same `timestamp` of the original account. This result can happen in
/// cases of application crashes or other scenarios where requests have been replayed.
///
/// [`create_accounts`]: Client::create_accounts
///
/// # Protocol reference
///
/// [`CreateAccountStatus`](https://docs.tigerbeetle.com/reference/requests/create_accounts/#result).
pub use tbc::CreateAccountStatus;

/// The result of a single [`create_accounts`] event.
///
/// [`create_accounts`]: Client::create_accounts
///
/// # Protocol reference
///
/// [`CreateAccountStatus`](https://docs.tigerbeetle.com/reference/requests/create_accounts/#result).
pub use tbc::CreateAccountResult;

/// The result of a single [`create_transfers`] event.
///
/// For the meaning of individual enum variants see the linked protocol reference.
///
/// See also [`CreateTransferResult`], the type directly returned by `create_transfers`,
/// which contains the timestamp at which the transfer was created. Note that a status of
/// `CreateTransferStatus::Exists` should often be treated like `CreateTransferStatus::Created`,
/// as it also returns the same `timestamp` of the original transfer. This result can happen in
/// cases of application crashes or other scenarios where requests have been replayed.
///
/// [`create_transfers`]: Client::create_transfers
///
/// # Protocol reference
///
/// [`CreateTransferStatus`](https://docs.tigerbeetle.com/reference/requests/create_transfers/#result).
pub use tbc::CreateTransferStatus;

/// The result of a single [`create_transfers`] event, with index.
///
/// [`create_transfers`]: Client::create_transfers
///
/// # Protocol reference
///
/// [`CreateTransferStatus`](https://docs.tigerbeetle.com/reference/requests/create_transfers/#result).
pub use tbc::CreateTransferResult;

/// Errors resulting from constructing a [`Client`].
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[non_exhaustive]
pub enum InitError {
    /// Some other unexpected error occurrred.
    Unexpected,
    /// Out of memory.
    OutOfMemory,
    /// There was some error parsing the provided addresses.
    AddressInvalid,
    /// Too many addresses were provided.
    AddressLimitExceeded,
    /// Some system resource was exhausted.
    ///
    /// This includes file descriptors, threads, and lockable memory.
    SystemResources,
    /// The network was unavailable or other network initialization error.
    NetworkSubsystem,
}

impl std::error::Error for InitError {}
impl core::fmt::Display for InitError {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Self::Unexpected => f.write_str("unexpected"),
            Self::OutOfMemory => f.write_str("out of memory"),
            Self::AddressInvalid => f.write_str("address invalid"),
            Self::AddressLimitExceeded => f.write_str("address limit exceeded"),
            Self::SystemResources => f.write_str("system resources"),
            Self::NetworkSubsystem => f.write_str("network subsystem"),
        }
    }
}

/// Errors that occur prior to the server processing a batch of operations.
///
/// When one of these is returned as a result of a transaction request,
/// then all operations in the request can be assumed to have not been processed.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[non_exhaustive]
pub enum PacketError {
    /// Too many events were submitted to a multi-event request.
    TooMuchData,
    /// The client was evicted by the server.
    ClientEvicted,
    /// The client's version is too low.
    ClientReleaseTooLow,
    /// The client's version is too high.
    ClientReleaseTooHigh,
    /// The client was closed.
    ClientClosed,
    /// An invalid operation was submitted.
    ///
    /// This should not be possible in the Rust client.
    InvalidOperation,
    /// The operation's payload was an incorrect size.
    ///
    /// This should not be possible in the Rust client.
    InvalidDataSize,
}

impl std::error::Error for PacketError {}
impl core::fmt::Display for PacketError {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Self::TooMuchData => f.write_str("too much data"),
            Self::ClientEvicted => f.write_str("client evicted"),
            Self::ClientReleaseTooLow => f.write_str("client release too low"),
            Self::ClientReleaseTooHigh => f.write_str("client release too high"),
            Self::ClientClosed => f.write_str("client closed"),
            Self::InvalidOperation => f.write_str("invalid operation"),
            Self::InvalidDataSize => f.write_str("invalid data size"),
        }
    }
}

/// An error type returned by point queries.
///
/// Returned by [`Client::lookup_accounts`] and [`Client::lookup_transfers`]
/// when the account or transfer does not exist.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NotFound;

impl std::error::Error for NotFound {}
impl core::fmt::Display for NotFound {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        f.write_str("not found")
    }
}

/// A utility type for representing reserved bytes in structs.
///
/// This type is instantiated with [`Default::default`] and typically
/// does not need to be used directly.
pub use tbc::Reserved;
