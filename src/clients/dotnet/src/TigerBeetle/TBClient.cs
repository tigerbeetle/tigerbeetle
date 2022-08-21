using System;
using System.Runtime.InteropServices;

namespace TigerBeetle
{
	internal static class TBClient
	{
		#region InnerTypes

		public enum TBPacketStatus : byte
		{
			Ok = 0,
			TooMuchData,
			InvalidOperation,
			InvalidDataSize,
		}

		[StructLayout(LayoutKind.Sequential)]
		public unsafe struct TBPacket
		{
			public TBPacket* next;
			public IntPtr user_data;
			public Operation operation;
			public TBPacketStatus status;
			public uint data_size;
			public IntPtr data;
		}

		public unsafe struct TBPacketList
		{
			public TBPacket* head;
			public TBPacket* tail;
		}

		public enum TBStatus: int
		{
			Success = 0,
			Unexpected = 1,
			OutOfMemory = 2,
			InvalidAddress = 3,
			SystemResources = 4,
			NetworkSubsystem = 5,
		}

		#endregion InnerTypes

		#region Fields

		private const string LIB_NAME = "tb_client";

		#endregion Fields

		#region Methods

#if NETSTANDARD

		[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
		public unsafe delegate void OnCompletionFn(IntPtr ctx, IntPtr client, Packet* packet, byte* result, uint result_len);

		[DllImport(LIB_NAME, CallingConvention = CallingConvention.Cdecl)]
		public static unsafe extern Status tb_client_init(
			IntPtr* out_client,
			PacketList* out_packets,
			uint cluster_id,
			byte* address_ptr,
			uint address_len,
			uint num_packets,
			IntPtr on_completion_ctx,

			[MarshalAs(UnmanagedType.FunctionPtr)]
			OnCompletionFn on_completion_fn
		);

#else

		[DllImport(LIB_NAME, CallingConvention = CallingConvention.Cdecl)]
		public static unsafe extern TBStatus tb_client_init(
			IntPtr* out_client,
			TBPacketList* out_packets,
			uint cluster_id,
			byte* address_ptr,
			uint address_len,
			uint num_packets,
			IntPtr on_completion_ctx,
			delegate* unmanaged[Cdecl]<IntPtr,IntPtr,TBPacket*,byte*,uint,void> on_completion_fn
		);

#endif

		[DllImport(LIB_NAME, CallingConvention = CallingConvention.Cdecl)]
		public static unsafe extern void tb_client_submit(
			IntPtr client,
			TBPacketList* packets
		);

		[DllImport(LIB_NAME, CallingConvention = CallingConvention.Cdecl)]
		public static unsafe extern void tb_client_deinit(
			IntPtr client
		);

#endregion Methods
	}
}
