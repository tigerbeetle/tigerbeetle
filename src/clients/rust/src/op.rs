//! Internal, type-erased request storage and generic submission machinery.

use std::{
    cell::UnsafeCell,
    ffi::c_void,
    future::Future,
    marker::PhantomData,
    mem::{self, MaybeUninit},
    ptr,
    sync::Arc,
    task::Poll,
};

use crate::{
    oneshot::CompletionCell,
    tb_client::{self as tbc, Operation},
    AccountFilter, Client, Completion, PacketError, Transfer,
};

type OpOutput<Op> = Result<
    (
        Completion,
        <Op as Operation>::OpSource,
        Vec<<Op as Operation>::OutputItem>,
    ),
    PacketError,
>;

struct OpAwaiting<Operation>(Option<Arc<OpState>>, PhantomData<fn() -> Operation>)
where
    Operation: tbc::Operation;

impl<Operation> Future for OpAwaiting<Operation>
where
    Operation: tbc::Operation,
{
    type Output = OpOutput<Operation>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let shared = self.0.as_ref().expect("polled operation after completion");
        match shared.poll::<Operation>(cx) {
            std::task::Poll::Ready(result) => {
                let shared = self.0.take().expect("operation state");
                std::task::Poll::Ready(
                    result.map(|(source, output)| (Completion { shared }, source, output)),
                )
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl Client {
    pub(super) fn execute<Operation>(
        &self,
        completion: Completion,
        source: Operation::OpSource,
        target: Vec<Operation::OutputItem>,
    ) -> impl Future<Output = OpOutput<Operation>>
    where
        Operation: tbc::Operation,
    {
        let Completion { shared } = completion;
        shared.prepare::<Operation>(source, target);
        let packet = shared.prepare_packet::<Operation>();
        let remote_arc = Arc::into_raw(Arc::clone(&shared));

        unsafe {
            let status = tbc::tb_client_submit(self.client, packet);
            match status {
                tbc::TB_CLIENT_STATUS_TB_CLIENT_SUCCESS => {
                    // Ownership of remote_arc transferred to the I/O thread.
                }
                tbc::TB_CLIENT_STATUS_TB_CLIENT_CLOSED => {
                    drop(Arc::from_raw(remote_arc));
                    shared.cancel(PacketError::ClientClosed);
                }
                tbc::TB_CLIENT_STATUS_TB_CLIENT_NOT_INITIALIZED => {
                    unreachable!("Client interface not initialized")
                }
                _ => {
                    unreachable!("unexpected status from tb_client_submit: {}", status)
                }
            }
        };

        OpAwaiting::<Operation>(Some(shared), PhantomData)
    }

    pub(super) fn execute_allocating<Operation>(
        &self,
        source: Operation::OpSource,
    ) -> impl Future<Output = Result<Vec<Operation::OutputItem>, PacketError>>
    where
        Operation: tbc::Operation,
    {
        let awaiting = self.execute::<Operation>(Completion::new(), source, Vec::new());
        async move { awaiting.await.map(|(_completion, _source, output)| output) }
    }
}

pub(crate) const COMPLETION_CONTEXT: usize = 0xAB;

const PAYLOAD_ALIGN_MAX: usize = 16;
const PAYLOAD_BYTES_MAX: usize = {
    let target_bytes_max = std::mem::size_of::<Vec<Transfer>>();
    let source_bytes_max = std::mem::size_of::<AccountFilter>();
    let padding_bytes_max = 8;
    target_bytes_max + source_bytes_max + padding_bytes_max
};

type Target<Operation> = Vec<<Operation as tbc::Operation>::OutputItem>;
type OperationResult<Operation> =
    Result<(<Operation as tbc::Operation>::OpSource, Target<Operation>), PacketError>;

#[repr(C)]
struct PayloadTyped<Operation: tbc::Operation> {
    source: Operation::OpSource,
    target: Target<Operation>,
}

impl<Operation: tbc::Operation> PayloadTyped<Operation> {
    const VTABLE: OpVTable = OpVTable {
        complete: Self::complete,
        drop: |storage| unsafe { PayloadBytes::cast::<Operation>(storage).drop_in_place() },
    };

    const ASSERT_FITS: () = {
        assert!(
            mem::align_of::<Self>() <= PAYLOAD_ALIGN_MAX,
            "TigerBeetle operation payload alignment exceeds 16 bytes"
        );
        assert!(
            mem::size_of::<Self>() <= PAYLOAD_BYTES_MAX,
            "TigerBeetle operation payload exceeds the maximum capacity"
        );
    };

    const fn assert_fits() {
        Self::ASSERT_FITS
    }

    unsafe fn complete(storage: *mut PayloadBytes, result_ptr: *const u8, result_len: u32) {
        let item_size = mem::size_of::<Operation::OutputItem>();
        assert!(item_size > 0);

        let results = match result_len {
            0 => &[],
            result_len => {
                let result_len = result_len as usize;
                assert!(result_len % item_size == 0);
                assert!(!result_ptr.is_null());

                let result_typed = result_ptr as *const Operation::OutputItem;
                let result_count = result_len / item_size;
                assert_eq!(
                    (result_typed as usize) % mem::align_of::<Operation::OutputItem>(),
                    0
                );
                std::slice::from_raw_parts(result_typed, result_count)
            }
        };

        (*PayloadBytes::cast::<Operation>(storage))
            .target
            .extend_from_slice(results);
    }
}

const _: () = PayloadTyped::<tbc::GetAccountTransfers>::assert_fits();

#[repr(C, align(16))]
struct PayloadBytes(MaybeUninit<[u8; PAYLOAD_BYTES_MAX]>);

impl PayloadBytes {
    const fn new() -> Self {
        Self(MaybeUninit::uninit())
    }

    fn cast<Operation: tbc::Operation>(storage: *mut Self) -> *mut PayloadTyped<Operation> {
        PayloadTyped::<Operation>::assert_fits();
        storage.cast()
    }
}

struct OpVTable {
    complete: unsafe fn(*mut PayloadBytes, *const u8, u32),
    drop: unsafe fn(*mut PayloadBytes),
}

// Owns (drops) the backing payload storage, which must outlive this and must not move.
// OpState stores this in its CompletionCell. The payload storage livesin the same Arc.
struct PayloadOwner {
    storage: *mut PayloadBytes,
    vtable: &'static OpVTable,
}

// SAFETY: all supported sources and targets are Send. Moving this does
// not move the payload itself and the pointer points to inside OpState.
unsafe impl Send for PayloadOwner {}

impl PayloadOwner {
    unsafe fn take<Operation: tbc::Operation>(self) -> (Operation::OpSource, Target<Operation>) {
        let owner = mem::ManuallyDrop::new(self);
        let PayloadTyped { source, target } = PayloadBytes::cast::<Operation>(owner.storage).read();
        (source, target)
    }
}

impl Drop for PayloadOwner {
    fn drop(&mut self) {
        unsafe { (self.vtable.drop)(self.storage) }
    }
}

#[repr(C)]
pub(super) struct OpState {
    // Fields drop in declaration order: destroy the payload owner before its backing storage.
    payload: CompletionCell<PayloadOwner, Result<PayloadOwner, PacketError>>,
    storage: UnsafeCell<PayloadBytes>,
    packet: UnsafeCell<tbc::tb_packet_t>,
}

// SAFETY: the completion cell synchronizes ownership and any access to the payload.
// The I/O thread can read the source until completion; addresses are stable in Arc.
// Packet is used by the I/O thread after submission and forgotten after completion.
unsafe impl Send for OpState {}
unsafe impl Sync for OpState {}

impl OpState {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            payload: CompletionCell::reusable(),
            packet: UnsafeCell::new(tbc::tb_packet_t {
                user_data: ptr::null_mut(),
                data: ptr::null_mut(),
                data_size: 0,
                user_tag: 0xABCD,
                operation: 0,
                status: tbc::TB_PACKET_STATUS_TB_PACKET_OK,
                opaque: [0; 64],
            }),
            storage: UnsafeCell::new(PayloadBytes::new()),
        })
    }

    fn prepare<Operation>(self: &Arc<Self>, source: Operation::OpSource, target: Target<Operation>)
    where
        Operation: tbc::Operation,
    {
        self.payload.reuse_with(|| unsafe {
            PayloadBytes::cast::<Operation>(self.storage.get())
                .write(PayloadTyped { source, target });
            PayloadOwner {
                storage: self.storage.get(),
                vtable: &PayloadTyped::<Operation>::VTABLE,
            }
        });
    }

    fn poll<Operation: tbc::Operation>(
        self: &Arc<Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<OperationResult<Operation>> {
        match self.payload.poll(cx) {
            Poll::Ready(Ok(owner)) => Poll::Ready(Ok(unsafe { owner.take::<Operation>() })),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn prepare_packet<Operation: tbc::Operation>(self: &Arc<Self>) -> *mut tbc::tb_packet_t {
        self.payload.with_awaiting(|owner| unsafe {
            // Take the pointer only after the source is in its stable Arc allocation since,
            // for unbatched operations, the packet points directly at, e.g., AccountFilter.
            let source = &(*PayloadBytes::cast::<Operation>(owner.storage)).source;
            let (source_ptr, source_size) = Operation::source_parts(source);
            let source_size = source_size.try_into().expect("input buffer too large");

            let packet = self.packet.get();
            *packet = tbc::tb_packet_t {
                user_data: Arc::as_ptr(self) as *mut c_void,
                data: source_ptr as *mut c_void,
                data_size: source_size,
                user_tag: 0xABCD,
                operation: Operation::OP_CODE,
                status: tbc::TB_PACKET_STATUS_TB_PACKET_OK,
                opaque: [0; 64],
            };

            packet
        })
    }

    fn cancel(&self, error: PacketError) {
        self.payload.complete_with(|_owner| Err(error));
    }
}

pub(crate) extern "C" fn on_completion(
    context: usize,
    packet: *mut tbc::tb_packet_t,
    _timestamp: u64,
    result_ptr: *const u8,
    result_len: u32,
) {
    unsafe {
        let status = (*packet).status;

        // This Arc is owned by the I/O thread and freed after typed completion returns.
        // If the calling thread has dropped the future, this will drop OpState as well.
        let state = Arc::from_raw((*packet).user_data as *const OpState);

        (*packet).data = ptr::null_mut();
        (*packet).user_data = ptr::null_mut();

        assert_eq!(context, COMPLETION_CONTEXT);
        assert_eq!(packet, state.packet.get());

        state.payload.complete_with(|owner| {
            if status != tbc::TB_PACKET_STATUS_TB_PACKET_OK {
                return Err(status.into());
            }
            (owner.vtable.complete)(owner.storage, result_ptr, result_len);
            Ok(owner)
        });
    }
}
