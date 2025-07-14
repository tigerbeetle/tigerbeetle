//! The official TigerBeetle client.
//!
//! This is a work-in-progress client for the [TigerBeetle] financial database.
//! It is not yet production-ready.
//!
//! [TigerBeetle]: https://tigerbeetle.com

#![allow(clippy::needless_lifetimes)] // explicit lifetimes for readability

use bitflags::bitflags;
use futures_channel::oneshot::{channel, Receiver};

use std::convert::Infallible;
use std::future::Future;
use std::os::raw::{c_char, c_void};
use std::{mem, ptr};

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

/// The tb_client completion context is unused by the Rust bindings.
/// This is just a magic number to jump out of logs.
const COMPLETION_CONTEXT: usize = 0xAB;

pub struct Client {
    client: *mut tbc::tb_client_t,
}

impl Client {
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

            collect_results(
                msg,
                |next_result: &tbc::tb_create_accounts_result_t,
                 _next_event: &Account,
                 event_index: usize|
                 -> bool { next_result.index as usize == event_index },
                CreateAccountResult::Ok,
                std::convert::identity,
            )
        }
    }

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

            collect_results(
                msg,
                |next_result: &tbc::tb_create_transfers_result_t,
                 _next_event: &Transfer,
                 event_index: usize|
                 -> bool { next_result.index as usize == event_index },
                CreateTransferResult::Ok,
                std::convert::identity,
            )
        }
    }

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

            collect_results(
                msg,
                |next_result: &Account, next_event: &u128, _event_index: usize| -> bool {
                    next_result.id == *next_event
                },
                Err(NotFound),
                Ok,
            )
        }
    }

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

            collect_results(
                msg,
                |next_result: &Transfer, next_event: &u128, _event_index: usize| -> bool {
                    next_result.id == *next_event
                },
                Err(NotFound),
                Ok,
            )
        }
    }

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

#[repr(C)]
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
    #[repr(transparent)]
    #[derive(Copy, Clone, Debug, Default)]
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

#[repr(C)]
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
    #[repr(transparent)]
    #[derive(Copy, Clone, Debug, Default)]
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

#[repr(C)]
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
    #[repr(transparent)]
    #[derive(Copy, Clone, Debug, Default)]
    pub struct AccountFilterFlags: u32 {
        const Debits = tbc::TB_ACCOUNT_FILTER_FLAGS_TB_ACCOUNT_FILTER_DEBITS;
        const Credits = tbc::TB_ACCOUNT_FILTER_FLAGS_TB_ACCOUNT_FILTER_CREDITS;
        const Reversed = tbc::TB_ACCOUNT_FILTER_FLAGS_TB_ACCOUNT_FILTER_REVERSED;
    }
}

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

#[repr(C)]
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
    #[repr(transparent)]
    #[derive(Copy, Clone, Debug, Default)]
    pub struct QueryFilterFlags: u32 {
        const Reversed = tbc::TB_QUERY_FILTER_FLAGS_TB_QUERY_FILTER_REVERSED;
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
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
            Self::CreditsPostedMustBeZero => f.write_str("credit_posted must zero"),
            Self::LedgerMustNotBeZero => f.write_str("ledger must not be zero"),
            Self::CodeMustNotBeZero => f.write_str("code must not be zero"),
            Self::ImportedEventTimestampMustNotRegress => {
                f.write_str("imported event timestamp must not regress")
            }
            Self::Unknown(code) => f.write_fmt(format_args!("unknown {code}")),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
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
    Unknown(u32),
}

impl core::fmt::Display for CreateTransferResult {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Self::Ok => f.write_str("ok"),
            Self::LinkedEventFailed => f.write_str("link event failed"),
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
            Self::ClosingTransferMustBePending => f.write_str("closing transfers must be pending"),
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
                f.write_str("pending transefer has different code")
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
            Self::CreditAccountAlreadyClosed => f.write_str("credit account alreday closed"),
            Self::OverflowsDebitsPending => f.write_str("overflows debits_pending"),
            Self::OverflowsCreditsPending => f.write_str("overflows credits_pending"),
            Self::OverflowsDebitsPosted => f.write_str("overflows debits_posted"),
            Self::OverflowsCreditsPosted => f.write_str("overflows credits_posted"),
            Self::OverflowsDebits => f.write_str("overflows debits"),
            Self::OverflowsCredits => f.write_str("overflows credits"),
            Self::OverflowsTimeout => f.write_str("overflows timeout"),
            Self::ExceedsCredits => f.write_str("exceeds credits"),
            Self::ExceedsDebits => f.write_str("exceeds debits"),
            Self::Unknown(code) => f.write_fmt(format_args!("unknown {code}")),
        }
    }
}

#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum InitStatus {
    Unexpected,
    OutOfMemory,
    AddressInvalid,
    AddressLimitExceeded,
    SystemResources,
    NetworkSubsystem,
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
            Self::Unknown(code) => f.write_fmt(format_args!("unknown {code}")),
        }
    }
}

#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum ClientStatus {
    Invalid,
    Unknown(i32),
}

impl std::error::Error for ClientStatus {}
impl core::fmt::Display for ClientStatus {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Self::Invalid => f.write_str("invalid"),
            Self::Unknown(code) => f.write_fmt(format_args!("unknown {code}")),
        }
    }
}

#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum PacketStatus {
    TooMuchData,
    ClientEvicted,
    ClientReleaseTooLow,
    ClientReleaseTooHigh,
    ClientShutdown,
    InvalidOperation,
    InvalidDataSize,
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
            Self::Unknown(code) => f.write_fmt(format_args!("unknown {code}")),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct NotFound;

impl std::error::Error for NotFound {}
impl core::fmt::Display for NotFound {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        f.write_str("not found")
    }
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone)]
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
fn collect_results<Event, CResult, RustResult, RustResultWrapper>(
    msg: CompletionMessage<Event>,
    result_is_for_event: fn(&CResult, &Event, usize) -> bool,
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
