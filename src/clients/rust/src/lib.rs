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
//! The cost of this though is that it does link to a non-Rust static library
//! (called `tb_client`), and it does need to context switch between threads for
//! every request. The native linking should be handled seamlessly on all
//! supported platforms, and the context switching overhead is expected to be
//! low compared to the cost of networking and disk I/O.
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
//! // Create accounts
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
//! assert_eq!(account_results[0], tb::CreateAccountResult::Ok);
//! assert_eq!(account_results[1], tb::CreateAccountResult::Ok);
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
//! assert_eq!(transfer_results[0], tb::CreateTransferResult::Ok);
//!
//! // Look up the accounts to see the transfer result
//! let accounts = client.lookup_accounts(&[account_id1, account_id2]).await?;
//! let account1 = accounts[0].unwrap();
//! let account2 = accounts[1].unwrap();
//!
//! assert_eq!(account1.debits_posted, 100);
//! assert_eq!(account2.credits_posted, 100);
//! # Ok(())
//! # }
//! ```
//!
//!
//! # Request batching
//!
//! Most transaction and query operations support multiple events of the same
//! type at once (this can be seen in the request method signatures accepting
//! slices of their input types) and it is strongly recommended to submit many
//! events in a single request at once as TigerBeetle will only reach its
//! performance limits when events are received in large batches. The client
//! _does_ implement its own internal batching and will attempt to create them
//! efficiently, but it is more efficient for applications to create their own
//! batches based on understanding of their own architectural needs and
//! limitations.
//!
//! In TigerBeetle's standard build-time configuration **the maximum number of
//! events per batch is 8189**. If the events in a request exceed this number
//! its future will return [`PacketStatus::TooMuchData`].
//!
//!
//! # Range query limits
//!
//! TigerBeetle's range queries, [`get_account_transfers`],
//! [`get_account_balances`], [`query_accounts`] and [`query_transfers`], also
//! have a limit to how many results they return.
//!
//! In TigerBeetle's standard build-time configuration **the maximum number of
//! results returned is 8189**.
//!
//! If the server returns a full batch for a range query, then further results
//! can be paged by incrementing `timeout_max` to one greater than the highest
//! timeout returned in the previous batch, and issuing a new query with
//! otherwise the same filter. This process can be repeated until the server
//! returns an unfull batch.
//!
//! [`get_account_transfers`]: `Client::get_account_transfers`
//! [`get_account_balances`]: `Client::get_account_balances`
//! [`query_accounts`]: `Client::query_accounts`
//! [`query_transfers`]: `Client::query_transfers`
//!
//! Here is an example of paging to get started with:
//!
//! ```no_run
//! use tigerbeetle as tb;
//! use futures::{stream, Stream};
//!
//! fn get_account_transfers_paged<'s>(
//!     client: &'s tb::Client,
//!     event: tb::AccountFilter,
//! ) -> impl Stream<Item = Result<Vec<tb::Transfer>, tb::PacketStatus>> + use<'s> {
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
//!     let is_reverse = event.flags.contains(tb::AccountFilterFlags::Reversed);
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
//!         let result_next = client.get_account_transfers(event).await;
//!         match result_next {
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
//!             Err(result_next) => Some((Err(result_next), State::End)),
//!         }
//!     })
//! }
//! ```
//!
//!
//! # Concurrency and multithreading
//!
//! Multiple requests may be submitted concurrently from a single client; the
//! results of which are returned as futures whose Rust lifetimes are tied to
//! the `Client`. The server only supports one in-flight request per client
//! though, so the client will internally buffer concurrent requests. To truly
//! have multiple requests in flight concurrently, multiple clients can be
//! created, though note that there is a hard-coded limit on how many clients
//! can be connected to the server simultaneously.
//!
//! The `Client` type implements `Send` and `Sync` and may be used in parallel
//! across multiple threads or async tasks, e.g. by placing it into an [`Arc`].
//! In some cases this may be useful because it allows the client to leverage
//! its internal request batching to batch events from multiple threads (or
//! tasks), but otherwise it provides no performance advantage.
//!
//! [`Arc`]: `std::sync::Arc`
//!
//!
//! # TigerBeetle time-based identifiers
//!
//! Accounts and transfers must have globally unique identifiers. The generation
//! of these is application-specific, and any scheme that guarantees unique IDs
//! will work. Barring other constraints, TigerBeetle recommends using
//! [TigerBeetle time-based identifiers][tbid]. This crate provides an
//! implementation in the [`id`] function.
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
//! codebase, you can use [`futures::executor::block_on`] to run async operations
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
//! Many types in this library are ABI-compatible with the underlying protocol
//! definition and can be cast (unsafely) directly to and from byte buffers
//! on all supported platforms, though this should not be required for typical
//! application purposes.
//!
//! The protocol-compatible types are:
//!
//! - [`Account`] and [`AccountFlags`]
//! - [`Transfer`] and [`TransferFlags`]
//! - [`AccountBalance`]
//! - [`AccountFilter`] and [`AccountFilterFlags`]
//! - [`QueryFilter`] and [`QueryFilterFlags`]
//!
//! Note that status enums are not ABI-compatible with the protocol's status codes
//! and must be converted with [`TryFrom`].
//!
//!
//! # References
//!
//! [The TigerBeetle Reference](https://docs.tigerbeetle.com/reference/).

#![allow(clippy::needless_lifetimes)] // explicit lifetimes for readability

use bitflags::bitflags;
use futures_channel::oneshot::{channel, Receiver};

use std::convert::Infallible;
use std::future::Future;
use std::os::raw::{c_char, c_void};
use std::{mem, ptr};

#[allow(unused)]
#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[rustfmt::skip]
mod tb_client;
use tb_client as tbc;

mod conversions;
mod time_based_id;

pub use time_based_id::id;

/// The tb_client completion context is unused by the Rust bindings.
/// This is just a magic number to jump out of logs.
const COMPLETION_CONTEXT: usize = 0xAB;

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
    pub fn new(cluster_id: u128, addresses: &str) -> Result<Client, InitStatus> {
        assert_abi_compatibility();

        unsafe {
            let tb_client = Box::new(tbc::tb_client_t {
                opaque: Default::default(),
            });
            let tb_client = Box::into_raw(tb_client);
            let status = tbc::tb_client_init(
                tb_client,
                &cluster_id.to_le_bytes(),
                addresses.as_ptr() as *const c_char,
                addresses.len() as u32,
                COMPLETION_CONTEXT,
                Some(on_completion),
            );
            if status == tbc::TB_INIT_STATUS_TB_INIT_SUCCESS {
                Ok(Client { client: tb_client })
            } else {
                Err(status.into())
            }
        }
    }

    /// Create one or more accounts.
    ///
    /// The request is queued for submission prior to return of this function;
    /// dropping the returned future will not cancel the request.
    ///
    /// Successful account creation will result in either [`CreateAccountResult::Ok`] or,
    /// in cases of application crashes or other scenarios where requests have
    /// been replayed, [`CreateAccountResult::Exists`]. It is often appropriate to
    /// treat these two cases identically.
    ///
    /// # Maximum batch size
    ///
    /// If the length of the `events` argument exceeds the maximum batch size
    /// the future will return [`Err`] of [`PacketStatus::TooMuchData`]. In
    /// TigerBeetle's standard build-time configuration the maximum batch size
    /// is 8189.
    ///
    /// # Errors
    ///
    /// This request has two levels of errors: if the entire request fails then
    /// the future returns [`Err`] of [`PacketStatus`] and the caller can assume
    /// that none of the submitted events were processed; if the request was
    /// processed, then each event has its own [`CreateAccountResult`], which
    /// may be an error status.
    ///
    /// # Protocol reference
    ///
    /// [`create_accounts`](https://docs.tigerbeetle.com/reference/requests/create_accounts).
    pub fn create_accounts<'s>(
        &'s self,
        events: &[Account],
    ) -> impl Future<Output = Result<Vec<CreateAccountResult>, PacketStatus>> + use<'s> {
        let (packet, rx) =
            create_packet::<Account>(tbc::TB_OPERATION_TB_OPERATION_CREATE_ACCOUNTS, events);

        unsafe {
            let status = tbc::tb_client_submit(self.client, Box::into_raw(packet));
            assert_eq!(status, tbc::TB_CLIENT_STATUS_TB_CLIENT_OK);
        }

        async {
            let msg = rx.await.expect("channel");

            collect_results::<Account, tbc::tb_create_accounts_result_t, CreateAccountResult>(
                msg,
                |next_result: &tbc::tb_create_accounts_result_t,
                 _next_event: &Account,
                 event_index: usize|
                 -> bool { next_result.index as usize == event_index },
                CreateAccountResult::Ok,
                |r: tbc::tb_create_accounts_result_t| {
                    // The server will never actually send this as it is the "hole" value.
                    assert!(r.result != tbc::TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_OK);
                    r.result.into()
                },
            )
        }
    }

    /// Create one or more transfers.
    ///
    /// The request is queued for submission prior to return of this function;
    /// dropping the returned future will not cancel the request.
    ///
    /// Successful transfer creation will result in either [`CreateTransferResult::Ok`] or,
    /// in cases of application crashes or other scenarios where requests have
    /// been replayed, [`CreateTransferResult::Exists`]. It is often appropriate to
    /// treat these two cases identically.
    ///
    /// # Maximum batch size
    ///
    /// If the length of the `events` argument exceeds the maximum batch size
    /// the future will return [`Err`] of [`PacketStatus::TooMuchData`]. In
    /// TigerBeetle's standard build-time configuration the maximum batch size
    /// is 8189.
    ///
    /// # Errors
    ///
    /// This request has two levels of errors: if the entire request fails then
    /// the future returns [`Err`] of [`PacketStatus`] and the caller can assume
    /// that none of the submitted events were processed; if the request was
    /// processed, then each event has its own [`CreateTransferResult`], which
    /// may be an error status.
    /// # Protocol reference
    ///
    /// [`create_transfers`](https://docs.tigerbeetle.com/reference/requests/create_transfers).
    pub fn create_transfers<'s>(
        &'s self,
        events: &[Transfer],
    ) -> impl Future<Output = Result<Vec<CreateTransferResult>, PacketStatus>> + use<'s> {
        let (packet, rx) =
            create_packet::<Transfer>(tbc::TB_OPERATION_TB_OPERATION_CREATE_TRANSFERS, events);

        unsafe {
            let status = tbc::tb_client_submit(self.client, Box::into_raw(packet));
            assert_eq!(status, tbc::TB_CLIENT_STATUS_TB_CLIENT_OK);
        }

        async {
            let msg = rx.await.expect("channel");

            collect_results::<Transfer, tbc::tb_create_transfers_result_t, CreateTransferResult>(
                msg,
                |next_result: &tbc::tb_create_transfers_result_t,
                 _next_event: &Transfer,
                 event_index: usize|
                 -> bool { next_result.index as usize == event_index },
                CreateTransferResult::Ok,
                |r: tbc::tb_create_transfers_result_t| {
                    // The server will never actually send this as it is the "hole" value.
                    assert!(r.result != tbc::TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_OK);
                    r.result.into()
                },
            )
        }
    }

    /// Query individual accounts.
    ///
    /// The request is queued for submission prior to return of this function;
    /// dropping the returned future will not cancel the request.
    ///
    /// # Maximum batch size
    ///
    /// If the length of the `events` argument exceeds the maximum batch size
    /// the future will return [`Err`] of [`PacketStatus::TooMuchData`]. In
    /// TigerBeetle's standard build-time configuration the maximum batch size
    /// is 8189.
    ///
    /// # Errors
    ///
    /// This request has two levels of errors: if the entire request fails then
    /// the future returns [`Err`] of [`PacketStatus`] and the caller can assume
    /// that none of the submitted events were processed; if the request was
    /// processed, then each event may possibly be [`NotFound`].
    ///
    /// # Protocol reference
    ///
    /// [`lookup_accounts`](https://docs.tigerbeetle.com/reference/requests/lookup_accounts).
    pub fn lookup_accounts<'s>(
        &'s self,
        events: &[u128],
    ) -> impl Future<Output = Result<Vec<Result<Account, NotFound>>, PacketStatus>> + use<'s> {
        let (packet, rx) =
            create_packet::<u128>(tbc::TB_OPERATION_TB_OPERATION_LOOKUP_ACCOUNTS, events);

        unsafe {
            let status = tbc::tb_client_submit(self.client, Box::into_raw(packet));
            assert_eq!(status, tbc::TB_CLIENT_STATUS_TB_CLIENT_OK);
        }

        async {
            let msg = rx.await.expect("channel");

            collect_results::<u128, Account, Result<Account, NotFound>>(
                msg,
                |next_result: &Account, next_event: &u128, _event_index: usize| -> bool {
                    next_result.id == *next_event
                },
                Err(NotFound),
                Ok,
            )
        }
    }

    /// Query individual transfers.
    ///
    /// The request is queued for submission prior to return of this function;
    /// dropping the returned future will not cancel the request.
    ///
    /// # Maximum batch size
    ///
    /// If the length of the `events` argument exceeds the maximum batch size
    /// the future will return [`Err`] of [`PacketStatus::TooMuchData`]. In
    /// TigerBeetle's standard build-time configuration the maximum batch size
    /// is 8189.
    ///
    /// # Errors
    ///
    /// This request has two levels of errors: if the entire request fails then
    /// the future returns [`Err`] of [`PacketStatus`] and the caller can assume
    /// that none of the submitted events were processed; if the request was
    /// processed, then each event may possibly be [`NotFound`].
    ///
    /// # Protocol reference
    ///
    /// [`lookup_transfers`](https://docs.tigerbeetle.com/reference/requests/lookup_transfers).
    pub fn lookup_transfers<'s>(
        &'s self,
        events: &[u128],
    ) -> impl Future<Output = Result<Vec<Result<Transfer, NotFound>>, PacketStatus>> + use<'s> {
        let (packet, rx) =
            create_packet::<u128>(tbc::TB_OPERATION_TB_OPERATION_LOOKUP_TRANSFERS, events);

        unsafe {
            let status = tbc::tb_client_submit(self.client, Box::into_raw(packet));
            assert_eq!(status, tbc::TB_CLIENT_STATUS_TB_CLIENT_OK);
        }

        async {
            let msg = rx.await.expect("channel");

            collect_results::<u128, Transfer, Result<Transfer, NotFound>>(
                msg,
                |next_result: &Transfer, next_event: &u128, _event_index: usize| -> bool {
                    next_result.id == *next_event
                },
                Err(NotFound),
                Ok,
            )
        }
    }

    /// Query multiple transfers for a single account.
    ///
    /// The request is queued for submission prior to return of this function;
    /// dropping the returned future will not cancel the request.
    ///
    /// # Errors
    ///
    /// If the entire request fails then the future returns [`Err`] of [`PacketStatus`].
    ///
    /// # Protocol reference
    ///
    /// [`get_account_transfers`](https://docs.tigerbeetle.com/reference/requests/get_account_transfers).
    pub fn get_account_transfers<'s>(
        &'s self,
        event: AccountFilter,
    ) -> impl Future<Output = Result<Vec<Transfer>, PacketStatus>> + use<'s> {
        let (packet, rx) = create_packet::<AccountFilter>(
            tbc::TB_OPERATION_TB_OPERATION_GET_ACCOUNT_TRANSFERS,
            &[event],
        );

        unsafe {
            let status = tbc::tb_client_submit(self.client, Box::into_raw(packet));
            assert_eq!(status, tbc::TB_CLIENT_STATUS_TB_CLIENT_OK);
        }

        async {
            let msg = rx.await.expect("channel");
            let result: &[Transfer] = handle_message(&msg)?;

            Ok(result.to_vec())
        }
    }

    /// Query historical account balances for a single account.
    ///
    /// The request is queued for submission prior to return of this function;
    /// dropping the returned future will not cancel the request.
    ///
    /// # Errors
    ///
    /// If the entire request fails then the future returns [`Err`] of [`PacketStatus`].
    ///
    /// # Protocol reference
    ///
    /// [`get_account_balances`](https://docs.tigerbeetle.com/reference/requests/get_account_balances).
    pub fn get_account_balances<'s>(
        &'s self,
        event: AccountFilter,
    ) -> impl Future<Output = Result<Vec<AccountBalance>, PacketStatus>> + use<'s> {
        let (packet, rx) = create_packet::<AccountFilter>(
            tbc::TB_OPERATION_TB_OPERATION_GET_ACCOUNT_BALANCES,
            &[event],
        );

        unsafe {
            let status = tbc::tb_client_submit(self.client, Box::into_raw(packet));
            assert_eq!(status, tbc::TB_CLIENT_STATUS_TB_CLIENT_OK);
        }

        async {
            let msg = rx.await.expect("channel");
            let result: &[AccountBalance] = handle_message(&msg)?;

            Ok(result.to_vec())
        }
    }

    /// Query multiple accounts related by fields and timestamps.
    ///
    /// The request is queued for submission prior to return of this function;
    /// dropping the returned future will not cancel the request.
    ///
    /// # Errors
    ///
    /// If the entire request fails then the future returns [`Err`] of [`PacketStatus`].
    ///
    /// # Protocol reference
    ///
    /// [`query_accounts`](https://docs.tigerbeetle.com/reference/requests/query_accounts).
    pub fn query_accounts<'s>(
        &'s self,
        event: QueryFilter,
    ) -> impl Future<Output = Result<Vec<Account>, PacketStatus>> + use<'s> {
        let (packet, rx) =
            create_packet::<QueryFilter>(tbc::TB_OPERATION_TB_OPERATION_QUERY_ACCOUNTS, &[event]);

        unsafe {
            let status = tbc::tb_client_submit(self.client, Box::into_raw(packet));
            assert_eq!(status, tbc::TB_CLIENT_STATUS_TB_CLIENT_OK);
        }

        async {
            let msg = rx.await.expect("channel");
            let result: &[Account] = handle_message(&msg)?;

            Ok(result.to_vec())
        }
    }

    /// Query multiple transfers related by fields and timestamps.
    ///
    /// The request is queued for submission prior to return of this function;
    /// dropping the returned future will not cancel the request.
    ///
    /// # Errors
    ///
    /// If the entire request fails then the future returns [`Err`] of [`PacketStatus`].
    ///
    /// # Protocol reference
    ///
    /// [`query_transfers`](https://docs.tigerbeetle.com/reference/requests/query_transfers).
    pub fn query_transfers<'s>(
        &'s self,
        event: QueryFilter,
    ) -> impl Future<Output = Result<Vec<Transfer>, PacketStatus>> + use<'s> {
        let (packet, rx) =
            create_packet::<QueryFilter>(tbc::TB_OPERATION_TB_OPERATION_QUERY_TRANSFERS, &[event]);

        unsafe {
            let status = tbc::tb_client_submit(self.client, Box::into_raw(packet));
            assert_eq!(status, tbc::TB_CLIENT_STATUS_TB_CLIENT_OK);
        }

        async {
            let msg = rx.await.expect("channel");
            let result: &[Transfer] = handle_message(&msg)?;

            Ok(result.to_vec())
        }
    }

    /// Close the client and asynchronously wait for completion.
    ///
    /// Note that it is not required for correctness to call this method &mdash;
    /// `Client`'s destructor will correctly shut down the client, though
    /// without providing the ability to wait for shutdown.
    ///
    /// This method is provided for those that wish to ensure that the client's
    /// outstanding requests and resources are completed and freed prior to proceeding.
    pub fn close(mut self) -> impl Future<Output = ()> {
        struct SendClient(*mut tbc::tb_client_t);
        unsafe impl Send for SendClient {}

        let client = std::mem::replace(&mut self.client, std::ptr::null_mut());
        let client = SendClient(client);

        let (tx, rx) = channel::<Infallible>();

        std::thread::spawn(move || {
            let client = client;
            unsafe {
                // This is a blocking function so we're calling it offthread.
                let status = tbc::tb_client_deinit(client.0);
                assert_eq!(status, tbc::TB_CLIENT_STATUS_TB_CLIENT_OK);
                std::mem::drop(Box::from_raw(client.0));
            }
            drop(tx);
        });

        async {
            // wait for the channel to close
            let _ = rx.await;
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if !self.client.is_null() {
            _ = Client {
                client: self.client,
            }
            .close();
        }
    }
}

/// Make basic assertions about the ABI of our types.
///
/// We don't actually use some of the C types at all,
/// instead casting directly to hand-written Rust types.
///
/// These assertions give us some confidence those types
/// might be possibly correct.
fn assert_abi_compatibility() {
    assert_eq!(
        std::mem::size_of::<Account>(),
        std::mem::size_of::<tbc::tb_account_t>()
    );
    assert_eq!(
        std::mem::align_of::<Account>(),
        std::mem::align_of::<tbc::tb_account_t>()
    );
    assert_eq!(
        std::mem::size_of::<Account>(),
        std::mem::size_of::<tbc::tb_account_t>()
    );
    assert_eq!(
        std::mem::align_of::<Account>(),
        std::mem::align_of::<tbc::tb_account_t>()
    );
    assert_eq!(
        std::mem::size_of::<Transfer>(),
        std::mem::size_of::<tbc::tb_transfer_t>()
    );
    assert_eq!(
        std::mem::align_of::<Transfer>(),
        std::mem::align_of::<tbc::tb_transfer_t>()
    );
    assert_eq!(
        std::mem::size_of::<Transfer>(),
        std::mem::size_of::<tbc::tb_transfer_t>()
    );
    assert_eq!(
        std::mem::align_of::<Transfer>(),
        std::mem::align_of::<tbc::tb_transfer_t>()
    );
    assert_eq!(
        std::mem::size_of::<AccountFilter>(),
        std::mem::size_of::<tbc::tb_account_filter_t>()
    );
    assert_eq!(
        std::mem::align_of::<AccountFilter>(),
        std::mem::align_of::<tbc::tb_account_filter_t>()
    );
    assert_eq!(
        std::mem::size_of::<AccountBalance>(),
        std::mem::size_of::<tbc::tb_account_balance_t>()
    );
    assert_eq!(
        std::mem::align_of::<AccountBalance>(),
        std::mem::align_of::<tbc::tb_account_balance_t>()
    );
    assert_eq!(
        std::mem::size_of::<QueryFilter>(),
        std::mem::size_of::<tbc::tb_query_filter_t>()
    );
    assert_eq!(
        std::mem::align_of::<QueryFilter>(),
        std::mem::align_of::<tbc::tb_query_filter_t>()
    );
}

/// A TigerBeetle account.
///
/// # Protocol reference
///
/// [`Account`](https://docs.tigerbeetle.com/reference/account/).
#[repr(C)]
#[derive(Debug, Copy, Clone, Default)]
pub struct Account {
    pub id: u128,
    pub debits_pending: u128,
    pub debits_posted: u128,
    pub credits_pending: u128,
    pub credits_posted: u128,
    pub user_data_128: u128,
    pub user_data_64: u64,
    pub user_data_32: u32,
    pub reserved: Reserved<4>,
    pub ledger: u32,
    pub code: u16,
    pub flags: AccountFlags,
    pub timestamp: u64,
}

bitflags! {
    /// Bitflags for the `flags` field of [`Account`].
    ///
    /// See the [`bitflags` crate](https://docs.rs/bitflags) for an explanation of Rust bitflags.
    ///
    /// # Protocol reference
    ///
    /// [`Account.flags`](https://docs.tigerbeetle.com/reference/account/#flags).
    #[repr(transparent)]
    #[derive(Copy, Clone, Debug, Default)]
    #[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
    pub struct AccountFlags: u16 {
        const None = 0;
        const Linked = tbc::TB_ACCOUNT_FLAGS_TB_ACCOUNT_LINKED;
        const DebitsMustNotExceedCredits = tbc::TB_ACCOUNT_FLAGS_TB_ACCOUNT_DEBITS_MUST_NOT_EXCEED_CREDITS;
        const CreditsMustNotExceedDebits = tbc::TB_ACCOUNT_FLAGS_TB_ACCOUNT_CREDITS_MUST_NOT_EXCEED_DEBITS;
        const History = tbc::TB_ACCOUNT_FLAGS_TB_ACCOUNT_HISTORY;
        const Imported = tbc::TB_ACCOUNT_FLAGS_TB_ACCOUNT_IMPORTED;
        const Closed = tbc::TB_ACCOUNT_FLAGS_TB_ACCOUNT_CLOSED;
    }
}

/// A transfer between accounts.
///
/// # Protocol reference
///
/// [`Transfer`](https://docs.tigerbeetle.com/reference/transfer).
#[repr(C)]
#[derive(Debug, Copy, Clone, Default)]
pub struct Transfer {
    pub id: u128,
    pub debit_account_id: u128,
    pub credit_account_id: u128,
    pub amount: u128,
    pub pending_id: u128,
    pub user_data_128: u128,
    pub user_data_64: u64,
    pub user_data_32: u32,
    pub timeout: u32,
    pub ledger: u32,
    pub code: u16,
    pub flags: TransferFlags,
    pub timestamp: u64,
}

bitflags! {
    /// Bitflags for the `flags` field of [`Transfer`].
    ///
    /// See the [`bitflags` crate](https://docs.rs/bitflags) for an explanation of Rust bitflags.
    ///
    /// # Protocol reference
    ///
    /// [`Transfer.flags`](https://docs.tigerbeetle.com/reference/transfer/#flags).
    #[repr(transparent)]
    #[derive(Copy, Clone, Debug, Default)]
    #[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
    pub struct TransferFlags: u16 {
        const Linked = tbc::TB_TRANSFER_FLAGS_TB_TRANSFER_LINKED;
        const Pending = tbc::TB_TRANSFER_FLAGS_TB_TRANSFER_PENDING;
        const PostPendingTransfer = tbc::TB_TRANSFER_FLAGS_TB_TRANSFER_POST_PENDING_TRANSFER;
        const VoidPendingTransfer = tbc::TB_TRANSFER_FLAGS_TB_TRANSFER_VOID_PENDING_TRANSFER;
        const BalancingDebit = tbc::TB_TRANSFER_FLAGS_TB_TRANSFER_BALANCING_DEBIT;
        const BalancingCredit = tbc::TB_TRANSFER_FLAGS_TB_TRANSFER_BALANCING_CREDIT;
        const ClosingDebit = tbc::TB_TRANSFER_FLAGS_TB_TRANSFER_CLOSING_DEBIT;
        const ClosingCredit = tbc::TB_TRANSFER_FLAGS_TB_TRANSFER_CLOSING_CREDIT;
        const Imported = tbc::TB_TRANSFER_FLAGS_TB_TRANSFER_IMPORTED;
    }
}

/// Filter for querying transfers and historical balances.
///
/// # Protocol reference
///
/// [`AccountFilter`](https://docs.tigerbeetle.com/reference/account-filter).
#[repr(C)]
#[derive(Debug, Copy, Clone, Default)]
pub struct AccountFilter {
    pub account_id: u128,
    pub user_data_128: u128,
    pub user_data_64: u64,
    pub user_data_32: u32,
    pub code: u16,
    pub reserved: Reserved<58>,
    pub timestamp_min: u64,
    pub timestamp_max: u64,
    pub limit: u32,
    pub flags: AccountFilterFlags,
}

bitflags! {
    /// Bitflags for the `flags` field of [`AccountFilter`].
    ///
    /// See the [`bitflags` crate](https://docs.rs/bitflags) for an explanation of Rust bitflags.
    ///
    /// # Protocol reference
    ///
    /// [`AccountFilter.flags`](https://docs.tigerbeetle.com/reference/account-filter/#flags).
    #[repr(transparent)]
    #[derive(Copy, Clone, Debug, Default)]
    #[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
    pub struct AccountFilterFlags: u32 {
        const Debits = tbc::TB_ACCOUNT_FILTER_FLAGS_TB_ACCOUNT_FILTER_DEBITS;
        const Credits = tbc::TB_ACCOUNT_FILTER_FLAGS_TB_ACCOUNT_FILTER_CREDITS;
        const Reversed = tbc::TB_ACCOUNT_FILTER_FLAGS_TB_ACCOUNT_FILTER_REVERSED;
    }
}

/// An account balance at a point in time.
///
/// # Protocol reference
///
/// [`AccountBalance`](https://docs.tigerbeetle.com/reference/account-balance/).
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct AccountBalance {
    pub debits_pending: u128,
    pub debits_posted: u128,
    pub credits_pending: u128,
    pub credits_posted: u128,
    pub timestamp: u64,
    pub reserved: Reserved<56>,
}

/// Parameters for querying accounts and transfers.
///
/// # Protocol reference
///
/// [`QueryFilter`](https://docs.tigerbeetle.com/reference/query-filter/).
#[repr(C)]
#[derive(Debug, Copy, Clone, Default)]
pub struct QueryFilter {
    pub user_data_128: u128,
    pub user_data_64: u64,
    pub user_data_32: u32,
    pub ledger: u32,
    pub code: u16,
    pub reserved: Reserved<6>,
    pub timestamp_min: u64,
    pub timestamp_max: u64,
    pub limit: u32,
    pub flags: QueryFilterFlags,
}

bitflags! {
    /// Bitflags for the `flags` field of [`QueryFilter`].
    ///
    /// See the [`bitflags` crate](https://docs.rs/bitflags) for an explanation of Rust bitflags.
    ///
    /// # Protocol reference
    ///
    /// [`QueryFilter.flags`](https://docs.tigerbeetle.com/reference/query-filter/#flags).
    #[repr(transparent)]
    #[derive(Copy, Clone, Debug, Default)]
    #[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
    pub struct QueryFilterFlags: u32 {
        const Reversed = tbc::TB_QUERY_FILTER_FLAGS_TB_QUERY_FILTER_REVERSED;
    }
}

/// The result of a single [`create_accounts`] event.
///
/// For the meaning of individual enum variants see the linked protocol reference.
///
/// [`create_accounts`]: `Client::create_accounts`
///
/// # Protocol reference
///
/// [`CreateAccountResult`](https://docs.tigerbeetle.com/reference/requests/create_accounts/#result).
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[non_exhaustive]
pub enum CreateAccountResult {
    Ok,
    LinkedEventFailed,
    LinkedEventChainOpen,
    ImportedEventExpected,
    ImportedEventNotExpected,
    TimestampMustBeZero,
    ImportedEventTimestampOutOfRange,
    ImportedEventTimestampMustNotAdvance,
    ReservedField,
    ReservedFlag,
    IdMustNotBeZero,
    IdMustNotBeIntMax,
    ExistsWithDifferentFlags,
    ExistsWithDifferentUserData128,
    ExistsWithDifferentUserData64,
    ExistsWithDifferentUserData32,
    ExistsWithDifferentLedger,
    ExistsWithDifferentCode,
    Exists,
    FlagsAreMutuallyExclusive,
    DebitsPendingMustBeZero,
    DebitsPostedMustBeZero,
    CreditsPendingMustBeZero,
    CreditsPostedMustBeZero,
    LedgerMustNotBeZero,
    CodeMustNotBeZero,
    ImportedEventTimestampMustNotRegress,
    /// An underlying subsystem returned an unknown status code.
    ///
    /// This exists for forward compatibility and other unexpected situations,
    /// and is unlikely to occur.
    Unknown(u32),
}

impl core::fmt::Display for CreateAccountResult {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Self::Ok => f.write_str("ok"),
            Self::LinkedEventFailed => f.write_str("linked event failed"),
            Self::LinkedEventChainOpen => f.write_str("linked event chain open"),
            Self::ImportedEventExpected => f.write_str("imported event expected"),
            Self::ImportedEventNotExpected => f.write_str("imported event not expected"),
            Self::TimestampMustBeZero => f.write_str("timestamp must be zero"),
            Self::ImportedEventTimestampOutOfRange => {
                f.write_str("imported event timestamp out of range")
            }
            Self::ImportedEventTimestampMustNotAdvance => {
                f.write_str("imported event timestamp must not advance")
            }
            Self::ReservedField => f.write_str("reserved field"),
            Self::ReservedFlag => f.write_str("reserved flag"),
            Self::IdMustNotBeZero => f.write_str("id must not be zero"),
            Self::IdMustNotBeIntMax => f.write_str("id must not be int max"),
            Self::ExistsWithDifferentFlags => f.write_str("exists with different flags"),
            Self::ExistsWithDifferentUserData128 => {
                f.write_str("exists with different user_data_128")
            }
            Self::ExistsWithDifferentUserData64 => {
                f.write_str("exists with different user_data_64")
            }
            Self::ExistsWithDifferentUserData32 => {
                f.write_str("exists with different user_data_32")
            }
            Self::ExistsWithDifferentLedger => f.write_str("exists with different ledger"),
            Self::ExistsWithDifferentCode => f.write_str("exists with different code"),
            Self::Exists => f.write_str("exists"),
            Self::FlagsAreMutuallyExclusive => f.write_str("flags are mutually exclusive"),
            Self::DebitsPendingMustBeZero => f.write_str("debits_pending must be zero"),
            Self::DebitsPostedMustBeZero => f.write_str("debits_posted must be zero"),
            Self::CreditsPendingMustBeZero => f.write_str("credits_pending must be zero"),
            Self::CreditsPostedMustBeZero => f.write_str("credits_posted must be zero"),
            Self::LedgerMustNotBeZero => f.write_str("ledger must not be zero"),
            Self::CodeMustNotBeZero => f.write_str("code must not be zero"),
            Self::ImportedEventTimestampMustNotRegress => {
                f.write_str("imported event timestamp must not regress")
            }
            Self::Unknown(code) => f.write_fmt(format_args!("unknown {0}", code)),
        }
    }
}

/// The result of a single [`create_transfers`] event.
///
/// For the meaning of individual enum variants see the linked protocol reference.
///
/// [`create_transfers`]: `Client::create_transfers`
///
/// # Protocol reference
///
/// [`CreateTransferResult`](https://docs.tigerbeetle.com/reference/requests/create_transfers/#result).
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[non_exhaustive]
pub enum CreateTransferResult {
    Ok,
    LinkedEventFailed,
    LinkedEventChainOpen,
    ImportedEventExpected,
    ImportedEventNotExpected,
    TimestampMustBeZero,
    ImportedEventTimestampOutOfRange,
    ImportedEventTimestampMustNotAdvance,
    ReservedFlag,
    IdMustNotBeZero,
    IdMustNotBeIntMax,
    ExistsWithDifferentFlags,
    ExistsWithDifferentPendingId,
    ExistsWithDifferentTimeout,
    ExistsWithDifferentDebitAccountId,
    ExistsWithDifferentCreditAccountId,
    ExistsWithDifferentAmount,
    ExistsWithDifferentUserData128,
    ExistsWithDifferentUserData64,
    ExistsWithDifferentUserData32,
    ExistsWithDifferentLedger,
    ExistsWithDifferentCode,
    Exists,
    IdAlreadyFailed,
    FlagsAreMutuallyExclusive,
    DebitAccountIdMustNotBeZero,
    DebitAccountIdMustNotBeIntMax,
    CreditAccountIdMustNotBeZero,
    CreditAccountIdMustNotBeIntMax,
    AccountsMustBeDifferent,
    PendingIdMustBeZero,
    PendingIdMustNotBeZero,
    PendingIdMustNotBeIntMax,
    PendingIdMustBeDifferent,
    TimeoutReservedForPendingTransfer,
    ClosingTransferMustBePending,
    LedgerMustNotBeZero,
    CodeMustNotBeZero,
    DebitAccountNotFound,
    CreditAccountNotFound,
    AccountsMustHaveTheSameLedger,
    TransferMustHaveTheSameLedgerAsAccounts,
    PendingTransferNotFound,
    PendingTransferNotPending,
    PendingTransferHasDifferentDebitAccountId,
    PendingTransferHasDifferentCreditAccountId,
    PendingTransferHasDifferentLedger,
    PendingTransferHasDifferentCode,
    ExceedsPendingTransferAmount,
    PendingTransferHasDifferentAmount,
    PendingTransferAlreadyPosted,
    PendingTransferAlreadyVoided,
    PendingTransferExpired,
    ImportedEventTimestampMustNotRegress,
    ImportedEventTimestampMustPostdateDebitAccount,
    ImportedEventTimestampMustPostdateCreditAccount,
    ImportedEventTimeoutMustBeZero,
    DebitAccountAlreadyClosed,
    CreditAccountAlreadyClosed,
    OverflowsDebitsPending,
    OverflowsCreditsPending,
    OverflowsDebitsPosted,
    OverflowsCreditsPosted,
    OverflowsDebits,
    OverflowsCredits,
    OverflowsTimeout,
    ExceedsCredits,
    ExceedsDebits,
    /// An underlying subsystem returned an unknown status code.
    ///
    /// This exists for forward compatibility and other unexpected situations,
    /// and is unlikely to occur.
    Unknown(u32),
}

impl core::fmt::Display for CreateTransferResult {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Self::Ok => f.write_str("ok"),
            Self::LinkedEventFailed => f.write_str("linked event failed"),
            Self::LinkedEventChainOpen => f.write_str("linked event chain open"),
            Self::ImportedEventExpected => f.write_str("imported event expected"),
            Self::ImportedEventNotExpected => f.write_str("imported event not expected"),
            Self::TimestampMustBeZero => f.write_str("timestamp must be zero"),
            Self::ImportedEventTimestampOutOfRange => {
                f.write_str("imported event timestamp out of range")
            }
            Self::ImportedEventTimestampMustNotAdvance => {
                f.write_str("imported event timestamp must not advance")
            }
            Self::ReservedFlag => f.write_str("reserved flag"),
            Self::IdMustNotBeZero => f.write_str("id must not be zero"),
            Self::IdMustNotBeIntMax => f.write_str("id must not be int max"),
            Self::ExistsWithDifferentFlags => f.write_str("exists with different flags"),
            Self::ExistsWithDifferentPendingId => f.write_str("exists with different pending_id"),
            Self::ExistsWithDifferentTimeout => f.write_str("exists with different timeout"),
            Self::ExistsWithDifferentDebitAccountId => {
                f.write_str("exists with different debit_account_id")
            }
            Self::ExistsWithDifferentCreditAccountId => {
                f.write_str("exists with different credit_account_id")
            }
            Self::ExistsWithDifferentAmount => f.write_str("exists with different amount"),
            Self::ExistsWithDifferentUserData128 => {
                f.write_str("exists with different user_data_128")
            }
            Self::ExistsWithDifferentUserData64 => {
                f.write_str("exists with different user_data_64")
            }
            Self::ExistsWithDifferentUserData32 => {
                f.write_str("exists with different user_data_32")
            }
            Self::ExistsWithDifferentLedger => f.write_str("exists with different ledger"),
            Self::ExistsWithDifferentCode => f.write_str("exists with different code"),
            Self::Exists => f.write_str("exists"),
            Self::IdAlreadyFailed => f.write_str("id already failed"),
            Self::FlagsAreMutuallyExclusive => f.write_str("flags are mutually exclusive"),
            Self::DebitAccountIdMustNotBeZero => f.write_str("debit_account_id must not be zero"),
            Self::DebitAccountIdMustNotBeIntMax => {
                f.write_str("debit_account_id must not be int max")
            }
            Self::CreditAccountIdMustNotBeZero => f.write_str("credit_account_id must not be zero"),
            Self::CreditAccountIdMustNotBeIntMax => {
                f.write_str("credit_account_id must not be int max")
            }
            Self::AccountsMustBeDifferent => f.write_str("accounts must be different"),
            Self::PendingIdMustBeZero => f.write_str("pending_id must be zero"),
            Self::PendingIdMustNotBeZero => f.write_str("pending_id must not be zero"),
            Self::PendingIdMustNotBeIntMax => f.write_str("pending_id must not be int max"),
            Self::PendingIdMustBeDifferent => f.write_str("pending_id must be different"),
            Self::TimeoutReservedForPendingTransfer => {
                f.write_str("timeout reserved for pending transfer")
            }
            Self::ClosingTransferMustBePending => f.write_str("closing transfer must be pending"),
            Self::LedgerMustNotBeZero => f.write_str("ledger must not be zero"),
            Self::CodeMustNotBeZero => f.write_str("code must not be zero"),
            Self::DebitAccountNotFound => f.write_str("debit account not found"),
            Self::CreditAccountNotFound => f.write_str("credit account not found"),
            Self::AccountsMustHaveTheSameLedger => {
                f.write_str("accounts must have the same ledger")
            }
            Self::TransferMustHaveTheSameLedgerAsAccounts => {
                f.write_str("transfer must have the same ledger as accounts")
            }
            Self::PendingTransferNotFound => f.write_str("pending transfer not found"),
            Self::PendingTransferNotPending => f.write_str("pending transfer not pending"),
            Self::PendingTransferHasDifferentDebitAccountId => {
                f.write_str("pending transfer has different debit_account_id")
            }
            Self::PendingTransferHasDifferentCreditAccountId => {
                f.write_str("pending transfer has different credit_account_id")
            }
            Self::PendingTransferHasDifferentLedger => {
                f.write_str("pending transfer has different ledger")
            }
            Self::PendingTransferHasDifferentCode => {
                f.write_str("pending transfer has different code")
            }
            Self::ExceedsPendingTransferAmount => f.write_str("exceeds pending transfer amount"),
            Self::PendingTransferHasDifferentAmount => {
                f.write_str("pending transfer has different amount")
            }
            Self::PendingTransferAlreadyPosted => f.write_str("pending transfer already posted"),
            Self::PendingTransferAlreadyVoided => f.write_str("pending transfer already voided"),
            Self::PendingTransferExpired => f.write_str("pending transfer expired"),
            Self::ImportedEventTimestampMustNotRegress => {
                f.write_str("imported event timestamp must not regress")
            }
            Self::ImportedEventTimestampMustPostdateDebitAccount => {
                f.write_str("imported event timestamp must postdate debit account")
            }
            Self::ImportedEventTimestampMustPostdateCreditAccount => {
                f.write_str("imported event timestamp must postdate credit account")
            }
            Self::ImportedEventTimeoutMustBeZero => {
                f.write_str("imported event timeout must be zero")
            }
            Self::DebitAccountAlreadyClosed => f.write_str("debit account already closed"),
            Self::CreditAccountAlreadyClosed => f.write_str("credit account already closed"),
            Self::OverflowsDebitsPending => f.write_str("overflows debits_pending"),
            Self::OverflowsCreditsPending => f.write_str("overflows credits_pending"),
            Self::OverflowsDebitsPosted => f.write_str("overflows debits_posted"),
            Self::OverflowsCreditsPosted => f.write_str("overflows credits_posted"),
            Self::OverflowsDebits => f.write_str("overflows debits"),
            Self::OverflowsCredits => f.write_str("overflows credits"),
            Self::OverflowsTimeout => f.write_str("overflows timeout"),
            Self::ExceedsCredits => f.write_str("exceeds credits"),
            Self::ExceedsDebits => f.write_str("exceeds debits"),
            Self::Unknown(code) => f.write_fmt(format_args!("unknown {0}", code)),
        }
    }
}

/// Errors resulting from constructing a [`Client`].
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[non_exhaustive]
pub enum InitStatus {
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
    /// An underlying subsystem returned an unknown status code.
    ///
    /// This exists for forward compatibility and other unexpected situations,
    /// and is unlikely to occur.
    Unknown(i32),
}

impl std::error::Error for InitStatus {}
impl core::fmt::Display for InitStatus {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Self::Unexpected => f.write_str("unexpected"),
            Self::OutOfMemory => f.write_str("out of memory"),
            Self::AddressInvalid => f.write_str("address invalid"),
            Self::AddressLimitExceeded => f.write_str("address limit exceeded"),
            Self::SystemResources => f.write_str("system resources"),
            Self::NetworkSubsystem => f.write_str("network subsystem"),
            Self::Unknown(code) => f.write_fmt(format_args!("unknown {0}", code)),
        }
    }
}

/// Errors that occur prior to the server processing a batch of operations.
///
/// When one of these is returned as a result of a transaction request,
/// then all operations in the request can be assumed to have not been processed.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[non_exhaustive]
pub enum PacketStatus {
    /// Too many events were submitted to a multi-event request.
    TooMuchData,
    /// The client was evicted by the server.
    ClientEvicted,
    /// The client's version is too low.
    ClientReleaseTooLow,
    /// The client's version is too high.
    ClientReleaseTooHigh,
    /// The client was already destructed.
    ///
    /// This should not be possible in the Rust client.
    ClientShutdown,
    /// An invalid operation was submitted.
    ///
    /// This should not be possible in the Rust client.
    InvalidOperation,
    /// The operation's payload was an incorrect size.
    ///
    /// This should not be possible in the Rust client.
    InvalidDataSize,
    /// An underlying subsystem returned an unknown status code.
    ///
    /// This exists for forward compatibility and other unexpected situations,
    /// and is unlikely to occur.
    Unknown(u8),
}

impl std::error::Error for PacketStatus {}
impl core::fmt::Display for PacketStatus {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Self::TooMuchData => f.write_str("too much data"),
            Self::ClientEvicted => f.write_str("client evicted"),
            Self::ClientReleaseTooLow => f.write_str("client release too low"),
            Self::ClientReleaseTooHigh => f.write_str("client release too high"),
            Self::ClientShutdown => f.write_str("client shutdown"),
            Self::InvalidOperation => f.write_str("invalid operation"),
            Self::InvalidDataSize => f.write_str("invalid data size"),
            Self::Unknown(code) => f.write_fmt(format_args!("unknown {0}", code)),
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
#[repr(transparent)]
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Reserved<const N: usize>([u8; N]);

impl<const N: usize> Default for Reserved<N> {
    fn default() -> Reserved<N> {
        Reserved([0; N])
    }
}

fn create_packet<Event>(
    op: u8, // TB_OPERATION
    events: &[Event],
) -> (Box<tbc::tb_packet_t>, Receiver<CompletionMessage<Event>>)
where
    Event: Copy + 'static,
{
    let (tx, rx) = channel::<CompletionMessage<Event>>();
    let callback: Box<OnCompletion> = Box::new(Box::new(
        |context, packet, timestamp, result_ptr, result_len| unsafe {
            let events_len = (*packet).data_size as usize / mem::size_of::<Event>();
            let events = Vec::from_raw_parts((*packet).data as *mut Event, events_len, events_len);
            (*packet).data = ptr::null_mut();

            let packet = Box::from_raw(packet);

            let result = if result_len != 0 {
                std::slice::from_raw_parts(result_ptr, result_len as usize)
            } else {
                &[]
            };
            let result = Vec::from(result);

            let _ = tx.send(CompletionMessage {
                _context: context,
                packet,
                _timestamp: timestamp,
                result,
                events,
            });
        },
    ));

    let mut events: Vec<Event> = events.to_vec();
    assert_eq!(events.len(), events.capacity());

    let events_len = events.len();
    let events_ptr = events.as_mut_ptr();
    mem::forget(events);

    let packet = Box::new(tbc::tb_packet_t {
        user_data: Box::into_raw(callback) as *mut c_void,
        data: events_ptr as *mut c_void,
        data_size: (mem::size_of::<Event>() * events_len) as u32,
        user_tag: 0xABCD,
        operation: op,
        status: tbc::TB_PACKET_STATUS_TB_PACKET_OK,
        opaque: Default::default(),
    });

    (packet, rx)
}

fn handle_message<'stack, CEvent, CResult>(
    msg: &'stack CompletionMessage<CEvent>,
) -> Result<&'stack [CResult], PacketStatus> {
    let packet = &msg.packet;
    let result = &msg.result;

    if packet.status != tbc::TB_PACKET_STATUS_TB_PACKET_OK {
        return Err(packet.status.into());
    }

    let result = unsafe {
        if !result.is_empty() {
            std::slice::from_raw_parts(
                result.as_ptr() as *const CResult,
                result
                    .len()
                    .checked_div(mem::size_of::<CResult>())
                    .expect("div"),
            )
        } else {
            &[]
        }
    };

    Ok(result)
}

/// Collect a vector of TigerBeetle query results into a vector of Rust values,
/// one for each input event.
///
/// Queries like account_lookup accept a vector of events, and the server
/// returns a vector of results.
///
/// These results though have "holes" in them, empty result values that need to
/// be interpreted in a specific way. Instead of having the user interpret the
/// holes in the results, we do it for them for every type of request.
///
/// This method does parallel iteration over the input events and the output
/// results as returned by tb_client, correlates the results to the events,
/// converts the result types to a Rust result type, and inserts new Rust
/// result types for each "hole" returned by the server.
///
/// # Type parameters
///
/// - `Event` - the input event type in the C API, e.g. `Account`
/// - `EventResponse` - the per-event result returned by the server, e.g.
///   `tb_create_accounts_result_t`
/// - `EventResult` - the final type returned by the Rust API, e.g.
///   `CreateAccountResult`
fn collect_results<Event, EventResponse, EventResult>(
    msg: CompletionMessage<Event>,
    response_is_for_event: fn(&EventResponse, &Event, usize) -> bool,
    empty_result: EventResult,
    nonempty_result: fn(EventResponse) -> EventResult,
) -> Result<Vec<EventResult>, PacketStatus>
where
    EventResponse: Copy,
    EventResult: Copy,
{
    let responses: &[EventResponse] = handle_message(&msg)?;
    let events = &msg.events;

    let mut result_accum: Vec<EventResult> = Vec::new();
    {
        let mut events_iter = events.iter().enumerate();
        let mut response_iter = responses.iter();

        let mut next_response = response_iter.next();

        loop {
            let next_event = events_iter.next();

            let Some((event_index, next_event)) = next_event else {
                assert!(next_response.is_none());
                break;
            };

            if let Some(next_response_) = next_response {
                let response_is_for_event =
                    response_is_for_event(next_response_, next_event, event_index);
                if response_is_for_event {
                    result_accum.push(nonempty_result(*next_response_));
                    next_response = response_iter.next();
                } else {
                    result_accum.push(empty_result);
                }
            } else {
                result_accum.push(empty_result);
            }
        }
    }

    assert_eq!(events.len(), result_accum.len());

    Ok(result_accum)
}

#[derive(Debug)]
struct CompletionMessage<E> {
    _context: usize,
    packet: Box<tbc::tb_packet_t>,
    _timestamp: u64,
    result: Vec<u8>,
    events: Vec<E>,
}

type OnCompletion = Box<dyn FnOnce(usize, *mut tbc::tb_packet_t, u64, *const u8, u32)>;

extern "C" fn on_completion(
    context: usize,
    packet: *mut tbc::tb_packet_t,
    timestamp: u64,
    result_ptr: *const u8,
    result_len: u32,
) {
    unsafe {
        let callback: Box<OnCompletion> = Box::from_raw((*packet).user_data as *mut OnCompletion);
        (*packet).user_data = ptr::null_mut();
        callback(context, packet, timestamp, result_ptr, result_len);
    }
}
