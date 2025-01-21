//! The official TigerBeetle client.
//!
//! This is a work-in-progress client for the [TigerBeetle] financial database.
//! It is not yet production-ready.
//!
//! [TigerBeetle]: https://tigerbeetle.com

#![allow(clippy::needless_lifetimes)] // explicit lifetimes for readability

use bitflags::bitflags;
use futures::channel::oneshot::{channel, Receiver};

use std::future::Future;
use std::os::raw::{c_char, c_void};
use std::{mem, ptr};

#[allow(unused)]
#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
mod tb_client;
use tb_client as tbc;

mod conversions;

/// The tb_client completion context is unused by the Rust bindings.
/// This is just a magic number to jump out of logs.
const COMPLETION_CONTEXT: usize = 0xAB;

pub struct Client {
    client: tbc::tb_client_t,
}

impl Client {
    pub fn new(cluster_id: u128, addresses: &str) -> Result<Client, Status> {
        unsafe {
            let mut tb_client = std::ptr::null_mut();
            let status = tb_client::tb_client_init(
                &mut tb_client,
                &cluster_id.to_le_bytes() as *const u8,
                addresses.as_ptr() as *const c_char,
                addresses.len() as u32,
                COMPLETION_CONTEXT,
                Some(on_completion),
            );
            if status == tb_client::TB_STATUS_TB_STATUS_SUCCESS {
                Ok(Client { client: tb_client })
            } else {
                Err(status.into())
            }
        }
    }

    pub fn submit_create_accounts<'s>(
        &'s self,
        events: &[Account],
    ) -> impl Future<Output = Result<Vec<Result<(), CreateAccountResult>>, PacketStatus>> + use<'s>
    {
        let (packet, rx) = create_packet::<Account, tbc::tb_account_t>(
            tbc::TB_OPERATION_TB_OPERATION_CREATE_ACCOUNTS,
            events,
        );

        unsafe {
            tbc::tb_client_submit(self.client, Box::into_raw(packet));
        }

        async {
            let msg = rx.await.expect("channel");

            collect_results(
                msg,
                |next_result: &tbc::tb_create_accounts_result_t,
                 _next_event: &tbc::tb_account_t,
                 event_index: usize|
                 -> bool { next_result.index as usize == event_index },
                Ok(()),
                Err,
            )
        }
    }

    pub fn submit_create_transfers<'s>(
        &'s self,
        events: &[Transfer],
    ) -> impl Future<Output = Result<Vec<Result<(), CreateTransferResult>>, PacketStatus>> + use<'s>
    {
        let (packet, rx) = create_packet::<Transfer, tbc::tb_transfer_t>(
            tbc::TB_OPERATION_TB_OPERATION_CREATE_TRANSFERS,
            events,
        );

        unsafe {
            tbc::tb_client_submit(self.client, Box::into_raw(packet));
        }

        async {
            let msg = rx.await.expect("channel");

            collect_results(
                msg,
                |next_result: &tbc::tb_create_transfers_result_t,
                 _next_event: &tbc::tb_transfer_t,
                 event_index: usize|
                 -> bool { next_result.index as usize == event_index },
                Ok(()),
                Err,
            )
        }
    }

    pub fn submit_lookup_accounts<'s>(
        &'s self,
        events: &[u128],
    ) -> impl Future<Output = Result<Vec<Result<Account, NotFound>>, PacketStatus>> + use<'s> {
        let (packet, rx) =
            create_packet::<u128, u128>(tbc::TB_OPERATION_TB_OPERATION_LOOKUP_ACCOUNTS, events);

        unsafe {
            tbc::tb_client_submit(self.client, Box::into_raw(packet));
        }

        async {
            let msg = rx.await.expect("channel");

            collect_results(
                msg,
                |next_result: &tbc::tb_account_t, next_event: &u128, _event_index: usize| -> bool {
                    next_result.id == *next_event
                },
                Err(NotFound),
                Ok,
            )
        }
    }

    pub fn submit_lookup_transfers<'s>(
        &'s self,
        events: &[u128],
    ) -> impl Future<Output = Result<Vec<Result<Transfer, NotFound>>, PacketStatus>> + use<'s> {
        let (packet, rx) =
            create_packet::<u128, u128>(tbc::TB_OPERATION_TB_OPERATION_LOOKUP_TRANSFERS, events);

        unsafe {
            tbc::tb_client_submit(self.client, Box::into_raw(packet));
        }

        async {
            let msg = rx.await.expect("channel");

            collect_results(
                msg,
                |next_result: &tbc::tb_transfer_t,
                 next_event: &u128,
                 _event_index: usize|
                 -> bool { next_result.id == *next_event },
                Err(NotFound),
                Ok,
            )
        }
    }

    pub fn submit_get_account_transfers<'s>(
        &'s self,
        event: AccountFilter,
    ) -> impl Future<Output = Result<Vec<Transfer>, PacketStatus>> + use<'s> {
        let (packet, rx) = create_packet::<AccountFilter, tbc::tb_account_filter_t>(
            tbc::TB_OPERATION_TB_OPERATION_GET_ACCOUNT_TRANSFERS,
            &[event],
        );

        unsafe {
            tbc::tb_client_submit(self.client, Box::into_raw(packet));
        }

        async {
            let msg = rx.await.expect("channel");
            let result: &[tbc::tb_transfer_t] = handle_message(&msg)?;

            Ok(result.iter().cloned().map(Into::into).collect())
        }
    }

    pub fn submit_get_account_balances<'s>(
        &'s self,
        event: AccountFilter,
    ) -> impl Future<Output = Result<Vec<AccountBalance>, PacketStatus>> + use<'s> {
        let (packet, rx) = create_packet::<AccountFilter, tbc::tb_account_filter_t>(
            tbc::TB_OPERATION_TB_OPERATION_GET_ACCOUNT_BALANCES,
            &[event],
        );

        unsafe {
            tbc::tb_client_submit(self.client, Box::into_raw(packet));
        }

        async {
            let msg = rx.await.expect("channel");
            let result: &[tbc::tb_account_balance_t] = handle_message(&msg)?;

            Ok(result.iter().cloned().map(Into::into).collect())
        }
    }

    pub fn submit_query_accounts<'s>(
        &'s self,
        event: QueryFilter,
    ) -> impl Future<Output = Result<Vec<Account>, PacketStatus>> + use<'s> {
        let (packet, rx) = create_packet::<QueryFilter, tbc::tb_query_filter_t>(
            tbc::TB_OPERATION_TB_OPERATION_QUERY_ACCOUNTS,
            &[event],
        );

        unsafe {
            tbc::tb_client_submit(self.client, Box::into_raw(packet));
        }

        async {
            let msg = rx.await.expect("channel");
            let result: &[tbc::tb_account_t] = handle_message(&msg)?;

            Ok(result.iter().cloned().map(Into::into).collect())
        }
    }

    pub fn submit_query_transfers<'s>(
        &'s self,
        event: QueryFilter,
    ) -> impl Future<Output = Result<Vec<Transfer>, PacketStatus>> + use<'s> {
        let (packet, rx) = create_packet::<QueryFilter, tbc::tb_query_filter_t>(
            tbc::TB_OPERATION_TB_OPERATION_QUERY_TRANSFERS,
            &[event],
        );

        unsafe {
            tbc::tb_client_submit(self.client, Box::into_raw(packet));
        }

        async {
            let msg = rx.await.expect("channel");
            let result: &[tbc::tb_transfer_t] = handle_message(&msg)?;

            Ok(result.iter().cloned().map(Into::into).collect())
        }
    }

    pub fn close(mut self) -> impl Future<Output = ()> {
        struct SendClient(tbc::tb_client_t);
        unsafe impl Send for SendClient {}

        let client = std::mem::replace(&mut self.client, std::ptr::null_mut());
        let client = SendClient(client);

        let (tx, rx) = channel::<()>();

        std::thread::spawn(move || {
            let client = client;
            unsafe {
                // This is a blocking function so we're calling it offthread.
                tbc::tb_client_deinit(client.0);
            }
            let _ = tx.send(());
        });

        async {
            rx.await.expect("destructor thread disappeared");
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

#[derive(Debug, Copy, Clone)]
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
    #[derive(Copy, Clone, Debug, Default)]
    pub struct AccountFlags: u16 {
        const None = 0;
        const Linked = tbc::TB_ACCOUNT_FLAGS_TB_ACCOUNT_LINKED as u16;
        const DebitsMustNotExceedCredits = tbc::TB_ACCOUNT_FLAGS_TB_ACCOUNT_DEBITS_MUST_NOT_EXCEED_CREDITS as u16;
        const CreditsMustNotExceedDebits = tbc::TB_ACCOUNT_FLAGS_TB_ACCOUNT_CREDITS_MUST_NOT_EXCEED_DEBITS as u16;
        const History = tbc::TB_ACCOUNT_FLAGS_TB_ACCOUNT_HISTORY as u16;
        const Imported = tbc::TB_ACCOUNT_FLAGS_TB_ACCOUNT_IMPORTED as u16;
        const Closed = tbc::TB_ACCOUNT_FLAGS_TB_ACCOUNT_CLOSED as u16;
    }
}

#[derive(Debug, Copy, Clone)]
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
    #[derive(Copy, Clone, Debug, Default)]
    pub struct TransferFlags: u16 {
        const Linked = tbc::TB_TRANSFER_FLAGS_TB_TRANSFER_LINKED as u16;
        const Pending = tbc::TB_TRANSFER_FLAGS_TB_TRANSFER_PENDING as u16;
        const PostPendingTransfer = tbc::TB_TRANSFER_FLAGS_TB_TRANSFER_POST_PENDING_TRANSFER as u16;
        const VoidPendingTransfer = tbc::TB_TRANSFER_FLAGS_TB_TRANSFER_VOID_PENDING_TRANSFER as u16;
        const BalancingDebit = tbc::TB_TRANSFER_FLAGS_TB_TRANSFER_BALANCING_DEBIT as u16;
        const BalancingCredit = tbc::TB_TRANSFER_FLAGS_TB_TRANSFER_BALANCING_CREDIT as u16;
        const ClosingDebit = tbc::TB_TRANSFER_FLAGS_TB_TRANSFER_CLOSING_DEBIT as u16;
        const ClosingCredit = tbc::TB_TRANSFER_FLAGS_TB_TRANSFER_CLOSING_CREDIT as u16;
        const Imported = tbc::TB_TRANSFER_FLAGS_TB_TRANSFER_IMPORTED as u16;
    }
}

#[derive(Debug, Copy, Clone)]
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
    #[derive(Copy, Clone, Debug, Default)]
    pub struct AccountFilterFlags: u32 {
        const Debits = tbc::TB_ACCOUNT_FILTER_FLAGS_TB_ACCOUNT_FILTER_DEBITS;
        const Credits = tbc::TB_ACCOUNT_FILTER_FLAGS_TB_ACCOUNT_FILTER_CREDITS;
        const Reversed = tbc::TB_ACCOUNT_FILTER_FLAGS_TB_ACCOUNT_FILTER_REVERSED;
    }
}

#[derive(Debug, Copy, Clone)]
pub struct AccountBalance {
    pub debits_pending: u128,
    pub debits_posted: u128,
    pub credits_pending: u128,
    pub credits_posted: u128,
    pub timestamp: u64,
    pub reserved: Reserved<56>,
}

#[derive(Debug, Copy, Clone)]
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
    #[derive(Copy, Clone, Debug, Default)]
    pub struct QueryFilterFlags: u32 {
        const Reversed = tbc::TB_QUERY_FILTER_FLAGS_TB_QUERY_FILTER_REVERSED;
    }
}

#[derive(thiserror::Error, Debug, Copy, Clone)]
#[non_exhaustive]
pub enum CreateAccountResult {
    #[error("linked event failed")]
    LinkedEventFailed,
    #[error("linked event chain open")]
    LinkedEventChainOpen,
    #[error("imported event expected")]
    ImportedEventExpected,
    #[error("imported event not expected")]
    ImportedEventNotExpected,
    #[error("timestamp must be zero")]
    TimestampMustBeZero,
    #[error("imported event timestamp out of range")]
    ImportedEventTimestampOutOfRange,
    #[error("imported event timestamp must not advance")]
    ImportedEventTimestampMustNotAdvance,
    #[error("reserved field")]
    ReservedField,
    #[error("reserved flag")]
    ReservedFlag,
    #[error("id must not be zero")]
    IdMustNotBeZero,
    #[error("id must not be int max")]
    IdMustNotBeIntMax,
    #[error("exists with different flags")]
    ExistsWithDifferentFlags,
    #[error("exists with different user_data_128")]
    ExistsWithDifferentUserData128,
    #[error("exists with different user_data_64")]
    ExistsWithDifferentUserData64,
    #[error("exists with different user_data_32")]
    ExistsWithDifferentUserData32,
    #[error("exists with different ledger")]
    ExistsWithDifferentLedger,
    #[error("exists with different code")]
    ExistsWithDifferentCode,
    #[error("exists")]
    Exists,
    #[error("flags are mutually exclusive")]
    FlagsAreMutuallyExclusive,
    #[error("debits_pending must be zero")]
    DebitsPendingMustBeZero,
    #[error("debits_posted must be zero")]
    DebitsPostedMustBeZero,
    #[error("credits_pending must be zero")]
    CreditsPendingMustBeZero,
    #[error("credit_posted must zero")]
    CreditsPostedMustBeZero,
    #[error("ledger must not be zero")]
    LedgerMustNotBeZero,
    #[error("code must not be zero")]
    CodeMustNotBeZero,
    #[error("imported event timestamp must not regress")]
    ImportedEventTimestampMustNotRegress,
    #[error("unknown {0}")]
    Unknown(u32),
}

#[derive(thiserror::Error, Debug, Copy, Clone)]
#[non_exhaustive]
pub enum CreateTransferResult {
    #[error("link event failed")]
    LinkedEventFailed,
    #[error("linked event chain open")]
    LinkedEventChainOpen,
    #[error("imported event expected")]
    ImportedEventExpected,
    #[error("imported event not expected")]
    ImportedEventNotExpected,
    #[error("timestamp must be zero")]
    TimestampMustBeZero,
    #[error("imported event timestamp out of range")]
    ImportedEventTimestampOutOfRange,
    #[error("imported event timestamp must not advance")]
    ImportedEventTimestampMustNotAdvance,
    #[error("reserved flag")]
    ReservedFlag,
    #[error("id must not be zero")]
    IdMustNotBeZero,
    #[error("id must not be int max")]
    IdMustNotBeIntMax,
    #[error("exists with different flags")]
    ExistsWithDifferentFlags,
    #[error("exists with different pending_id")]
    ExistsWithDifferentPendingId,
    #[error("exists with different timeout")]
    ExistsWithDifferentTimeout,
    #[error("exists with different debit_account_id")]
    ExistsWithDifferentDebitAccountId,
    #[error("exists with different credit_account_id")]
    ExistsWithDifferentCreditAccountId,
    #[error("exists with different amount")]
    ExistsWithDifferentAmount,
    #[error("exists with different user_data_128")]
    ExistsWithDifferentUserData128,
    #[error("exists with different user_data_64")]
    ExistsWithDifferentUserData64,
    #[error("exists with different user_data_32")]
    ExistsWithDifferentUserData32,
    #[error("exists with different ledger")]
    ExistsWithDifferentLedger,
    #[error("exists with different code")]
    ExistsWithDifferentCode,
    #[error("exists")]
    Exists,
    #[error("id already failed")]
    IdAlreadyFailed,
    #[error("flags are mutually exclusive")]
    FlagsAreMutuallyExclusive,
    #[error("debit_account_id must not be zero")]
    DebitAccountIdMustNotBeZero,
    #[error("debit_account_id must not be int max")]
    DebitAccountIdMustNotBeIntMax,
    #[error("credit_account_id must not be zero")]
    CreditAccountIdMustNotBeZero,
    #[error("credit_account_id must not be int max")]
    CreditAccountIdMustNotBeIntMax,
    #[error("accounts must be different")]
    AccountsMustBeDifferent,
    #[error("pending_id must be zero")]
    PendingIdMustBeZero,
    #[error("pending_id must not be zero")]
    PendingIdMustNotBeZero,
    #[error("pending_id must not be int max")]
    PendingIdMustNotBeIntMax,
    #[error("pending_id must be different")]
    PendingIdMustBeDifferent,
    #[error("timeout reserved for pending transfer")]
    TimeoutReservedForPendingTransfer,
    #[error("closing transfers must be pending")]
    ClosingTransferMustBePending,
    #[error("amount must not be zero")]
    AmountMustNotBeZero,
    #[error("ledger must not be zero")]
    LedgerMustNotBeZero,
    #[error("code must not be zero")]
    CodeMustNotBeZero,
    #[error("debit account not found")]
    DebitAccountNotFound,
    #[error("credit account not found")]
    CreditAccountNotFound,
    #[error("accounts must have the same ledger")]
    AccountsMustHaveTheSameLedger,
    #[error("transfer must have the same ledger as accounts")]
    TransferMustHaveTheSameLedgerAsAccounts,
    #[error("pending transfer not found")]
    PendingTransferNotFound,
    #[error("pending transfer not pending")]
    PendingTransferNotPending,
    #[error("pending transfer has different debit_account_id")]
    PendingTransferHasDifferentDebitAccountId,
    #[error("pending transfer has different credit_account_id")]
    PendingTransferHasDifferentCreditAccountId,
    #[error("pending transfer has different ledger")]
    PendingTransferHasDifferentLedger,
    #[error("pending transefer has different code")]
    PendingTransferHasDifferentCode,
    #[error("exceeds pending transfer amount")]
    ExceedsPendingTransferAmount,
    #[error("pending transfer has different amount")]
    PendingTransferHasDifferentAmount,
    #[error("pending transfer already posted")]
    PendingTransferAlreadyPosted,
    #[error("pending transfer already voided")]
    PendingTransferAlreadyVoided,
    #[error("pending transfer expired")]
    PendingTransferExpired,
    #[error("imported event timestamp must not regress")]
    ImportedEventTimestampMustNotRegress,
    #[error("imported event timestamp must postdate debit account")]
    ImportedEventTimestampMustPostdateDebitAccount,
    #[error("imported event timestamp must postdate credit account")]
    ImportedEventTimestampMustPostdateCreditAccount,
    #[error("imported event timeout must be zero")]
    ImportedEventTimeoutMustBeZero,
    #[error("debit account already closed")]
    DebitAccountAlreadyClosed,
    #[error("credit account alreday closed")]
    CreditAccountAlreadyClosed,
    #[error("overflows debits_pending")]
    OverflowsDebitsPending,
    #[error("overflows credits_pending")]
    OverflowsCreditsPending,
    #[error("overflows debits_posted")]
    OverflowsDebitsPosted,
    #[error("overflows credits_posted")]
    OverflowsCreditsPosted,
    #[error("overflows debits")]
    OverflowsDebits,
    #[error("overflows credits")]
    OverflowsCredits,
    #[error("overflows timeout")]
    OverflowsTimeout,
    #[error("exceeds credits")]
    ExceedsCredits,
    #[error("exceeds debits")]
    ExceedsDebits,
    #[error("unknown {0}")]
    Unknown(u32),
}

#[derive(thiserror::Error, Debug, Copy, Clone)]
#[non_exhaustive]
pub enum Status {
    #[error("unexpected")]
    Unexpected,
    #[error("out of memory")]
    OutOfMemory,
    #[error("address invalid")]
    AddressInvalid,
    #[error("address limit exceeded")]
    AddressLimitExceeded,
    #[error("system resources")]
    SystemResources,
    #[error("network subsystem")]
    NetworkSubsystem,
    #[error("unknown {0}")]
    Unknown(u32),
}

#[derive(thiserror::Error, Debug, Copy, Clone)]
#[non_exhaustive]
pub enum PacketStatus {
    #[error("too much data")]
    TooMuchData,
    #[error("client evicted")]
    ClientEvicted,
    #[error("client release too low")]
    ClientReleaseTooLow,
    #[error("client release too high")]
    ClientReleaseTooHigh,
    #[error("client shutdown")]
    ClientShutdown,
    #[error("invalid operation")]
    InvalidOperation,
    #[error("invalid data size")]
    InvalidDataSize,
    #[error("unknown {0}")]
    Unknown(u32),
}

#[derive(thiserror::Error, Debug, Copy, Clone)]
#[error("not found")]
pub struct NotFound;

#[derive(Debug, Copy, Clone)]
pub struct Reserved<const N: usize>([u8; N]);

impl<const N: usize> Default for Reserved<N> {
    fn default() -> Reserved<N> {
        Reserved([0; N])
    }
}

fn create_packet<RustEvent, CEvent>(
    op: u32, // TB_OPERATION
    events: &[RustEvent],
) -> (Box<tbc::tb_packet_t>, Receiver<CompletionMessage<CEvent>>)
where
    RustEvent: Copy,
    CEvent: From<RustEvent> + 'static,
{
    let (tx, rx) = channel::<CompletionMessage<CEvent>>();
    let callback: Box<OnCompletion> = Box::new(Box::new(
        |context, client, packet, timestamp, result_ptr, result_len| unsafe {
            let events_len = (*packet).data_size as usize / mem::size_of::<CEvent>();
            let events = Vec::from_raw_parts((*packet).data as *mut CEvent, events_len, events_len);
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
                _client: client,
                packet,
                _timestamp: timestamp,
                result,
                events,
            });
        },
    ));

    let mut events: Vec<CEvent> = events.iter().copied().map(From::from).collect();
    assert_eq!(events.len(), events.capacity());

    let events_len = events.len();
    let events_ptr = events.as_mut_ptr();
    mem::forget(events);

    let packet = Box::new(tbc::tb_packet_t {
        next: ptr::null_mut(),
        user_data: Box::into_raw(callback) as *mut c_void,
        operation: op as u8,
        status: tbc::TB_PACKET_STATUS_TB_PACKET_OK as u8,
        data_size: (mem::size_of::<CEvent>() * events_len) as u32,
        data: events_ptr as *mut c_void,
        batch_next: ptr::null_mut(),
        batch_tail: ptr::null_mut(),
        batch_size: 0,
        batch_allowed: 0,
        reserved: Default::default(),
    });

    (packet, rx)
}

fn handle_message<'stack, CEvent, CResult>(
    msg: &'stack CompletionMessage<CEvent>,
) -> Result<&'stack [CResult], PacketStatus> {
    let packet = &msg.packet;
    let result = &msg.result;

    if packet.status != tbc::TB_PACKET_STATUS_TB_PACKET_OK as u8 {
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

/// Collect a vector of C-API tigerbeetle query results
/// into a vector of Rust values, one for each input event.
///
/// Queries like account_lookup accept a vector of events,
/// and the server returns a vector of results.
///
/// These results though have "holes" in them, empty result
/// values that need to be interpreted in a specific way.
/// Instead of having the user interpret the holes in the results,
/// we do it for them for every type of request.
///
/// This method does parallel iteration over the input events
/// and the output results as returned by tb_client,
/// correlates the results to the events, converts the C result
/// types to a Rust result type, and inserts new Rust result types
/// for each "hole" returned by the server.
///
/// # Type parameters
///
/// - `CEvent` - the input event type in the C API,
///   e.g. `tb_account_t`
/// - `CResult` - the result returned by the server,
///   e.g. `tb_create_accounts_result_t`
/// - `RustResult` - the corresponding Rust definition for `CResult`.
///   Note this is not a Rust `Result` type, the "Result" name is
///   in relation to the `CResult` type, which have `_result` names
///   in the C API.
///   e.g. `CreateAccountResult`
/// - `RustResultWrapper` - the final response for each event, created
///   from `RustResult`. This probably _is_ a Rust `Result` type!
fn collect_results<CEvent, CResult, RustResult, RustResultWrapper>(
    msg: CompletionMessage<CEvent>,
    result_is_for_event: fn(&CResult, &CEvent, usize) -> bool,
    empty_result: RustResultWrapper,
    nonempty_result: fn(RustResult) -> RustResultWrapper,
) -> Result<Vec<RustResultWrapper>, PacketStatus>
where
    CResult: Copy,
    RustResult: From<CResult>,
    RustResultWrapper: Copy,
{
    let result: &[CResult] = handle_message(&msg)?;
    let events = &msg.events;

    let mut result_accum: Vec<RustResultWrapper> = Vec::new();
    {
        let mut events_iter = events.iter().enumerate();
        let mut result_iter = result.iter();

        let mut next_result = result_iter.next();

        loop {
            let next_event = events_iter.next();

            let Some((event_index, next_event)) = next_event else {
                assert!(next_result.is_none());
                break;
            };

            if let Some(next_result_) = next_result {
                let result_is_for_event =
                    result_is_for_event(next_result_, next_event, event_index);
                if result_is_for_event {
                    result_accum.push(nonempty_result((*next_result_).into()));
                    next_result = result_iter.next();
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
    _client: tbc::tb_client_t,
    packet: Box<tbc::tb_packet_t>,
    _timestamp: u64,
    result: Vec<u8>,
    events: Vec<E>,
}

type OnCompletion =
    Box<dyn FnOnce(usize, tbc::tb_client_t, *mut tbc::tb_packet_t, u64, *const u8, u32)>;

extern "C" fn on_completion(
    context: usize,
    client: tbc::tb_client_t,
    packet: *mut tbc::tb_packet_t,
    timestamp: u64,
    result_ptr: *const u8,
    result_len: u32,
) {
    unsafe {
        let callback: Box<OnCompletion> = Box::from_raw((*packet).user_data as *mut OnCompletion);
        (*packet).user_data = ptr::null_mut();
        callback(context, client, packet, timestamp, result_ptr, result_len);
    }
}
