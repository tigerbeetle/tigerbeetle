using System;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

using static TigerBeetle.TBClient;

[assembly: InternalsVisibleTo("TigerBeetle.Tests")]

namespace TigerBeetle
{
    public sealed class Client : IDisposable
    {
        #region Fields

        private IntPtr handle;
        private readonly uint clusterID;

        private readonly PacketList packets;

        #endregion Fields

        #region Constructor

        public Client(uint clusterID, IPEndPoint[] configuration, int maxConcurrency)
        {
            if (configuration == null || configuration.Length == 0) throw new ArgumentException(nameof(configuration));

            // Cap the maximum amount of packets
            if (maxConcurrency <= 0) throw new ArgumentException(nameof(maxConcurrency));
            if (maxConcurrency > 4096) maxConcurrency = 4096;

            this.clusterID = clusterID;

            var addresses_byte = Encoding.UTF8.GetBytes(string.Join(',', configuration.Select(x => x.ToString())) + '\0');
            unsafe
            {
                fixed (byte* addressPtr = addresses_byte)
                {
                    IntPtr handle;
                    TBPacketList packetList;

#if NETSTANDARD
					var status = PInvoke.tb_client_init(&handle, &packetList, clusterID, addressPtr, (uint)addresses_byte.Length - 1, (uint)maxConcurrency, IntPtr.Zero, OnCompletionHandler);
#else
                    var status = tb_client_init(&handle, &packetList, clusterID, addressPtr, (uint)addresses_byte.Length - 1, (uint)maxConcurrency, IntPtr.Zero, &OnCompletionCallback);
#endif

                    if (status != TBStatus.Success) throw new Exception($"Result {status}");

                    this.handle = handle;
                    this.packets = new PacketList(this, packetList, maxConcurrency);
                }
            }
        }

        ~Client()
        {
            Dispose(disposing: false);
        }

        #endregion Constructor

        #region Properties

        public IntPtr Handle => handle;

        public uint ClusterID => clusterID;

        internal PacketList Packets => packets;

        #endregion Properties

        #region Methods

        public CreateAccountResult CreateAccount(Account account)
        {
            var ret = CallRequest<CreateAccountsResult, Account>(Operation.CreateAccounts, new[] { account });
            return ret.Length == 0 ? CreateAccountResult.Ok : ret[0].Result;
        }

        public CreateAccountsResult[] CreateAccounts(Account[] batch)
        {
            return CallRequest<CreateAccountsResult, Account>(Operation.CreateAccounts, batch);
        }

        public async Task<CreateAccountResult> CreateAccountAsync(Account account)
        {
            var result = await CallRequestAsync<CreateAccountsResult, Account>(Operation.CreateAccounts, new[] { account });
            return result.Length == 0 ? CreateAccountResult.Ok : result[0].Result;
        }

        public Task<CreateAccountsResult[]> CreateAccountsAsync(Account[] batch)
        {
            return CallRequestAsync<CreateAccountsResult, Account>(Operation.CreateAccounts, batch);
        }

        public CreateTransferResult CreateTransfer(Transfer transfer)
        {
            var ret = CallRequest<CreateTransfersResult, Transfer>(Operation.CreateTransfers, new[] { transfer });
            return ret.Length == 0 ? CreateTransferResult.Ok : ret[0].Result;
        }

        public CreateTransfersResult[] CreateTransfers(Transfer[] batch)
        {
            return CallRequest<CreateTransfersResult, Transfer>(Operation.CreateTransfers, batch);
        }

        public async Task<CreateTransferResult> CreateTransferAsync(Transfer transfer)
        {
            var result = await CallRequestAsync<CreateTransfersResult, Transfer>(Operation.CreateTransfers, new[] { transfer });
            return result.Length == 0 ? CreateTransferResult.Ok : result[0].Result;
        }

        public Task<CreateTransfersResult[]> CreateTransfersAsync(Transfer[] batch)
        {
            return CallRequestAsync<CreateTransfersResult, Transfer>(Operation.CreateTransfers, batch);
        }

        public Account? LookupAccount(UInt128 id)
        {
            var ret = CallRequest<Account, UInt128>(Operation.LookupAccounts, new[] { id });
            return ret.Length == 0 ? null : ret[0];
        }

        public Account[] LookupAccounts(UInt128[] ids)
        {
            return CallRequest<Account, UInt128>(Operation.LookupAccounts, ids);
        }

        public async Task<Account?> LookupAccountAsync(UInt128 id)
        {
            var result = await CallRequestAsync<Account, UInt128>(Operation.LookupAccounts, new[] { id });
            return result.Length == 0 ? null : result[0];
        }

        public Task<Account[]> LookupAccountsAsync(UInt128[] ids)
        {
            return CallRequestAsync<Account, UInt128>(Operation.LookupAccounts, ids);
        }

        public Transfer? LookupTransfer(UInt128 id)
        {
            var ret = CallRequest<Transfer, UInt128>(Operation.LookupTransfers, new[] { id });
            return ret.Length == 0 ? null : ret[0];
        }

        public Transfer[] LookupTransfers(UInt128[] ids)
        {
            return CallRequest<Transfer, UInt128>(Operation.LookupTransfers, ids);
        }

        public async Task<Transfer?> LookupTransferAsync(UInt128 id)
        {
            var result = await CallRequestAsync<Transfer, UInt128>(Operation.LookupTransfers, new[] { id });
            return result.Length == 0 ? null : result[0];
        }

        public Task<Transfer[]> LookupTransfersAsync(UInt128[] ids)
        {
            return CallRequestAsync<Transfer, UInt128>(Operation.LookupTransfers, ids);
        }

        private TResult[] CallRequest<TResult, TBody>(Operation operation, TBody[] batch)
            where TResult : unmanaged
            where TBody : unmanaged
        {
            var packet = packets.Rent();
            var blockingRequest = new BlockingRequest<TResult, TBody>(this, packet);

            blockingRequest.Submit(operation, batch);
            return blockingRequest.Wait();
        }

        private async Task<TResult[]> CallRequestAsync<TResult, TBody>(Operation operation, TBody[] batch)
            where TResult : unmanaged
            where TBody : unmanaged
        {
            var packet = await packets.RentAsync();
            var asyncRequest = new AsyncRequest<TResult, TBody>(this, packet);

            asyncRequest.Submit(operation, batch);
            return await asyncRequest.Wait();
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            Dispose(disposing: true);
        }

        private void Dispose(bool disposing)
        {
            _ = disposing;

            if (handle != IntPtr.Zero)
            {
                tb_client_deinit(handle);
                handle = IntPtr.Zero;
            }
        }

        #endregion Methods

        #region TBClient callback

        #region Comments

        // Uses either the new function pointer by value, or the old managed delegate in .Net standard
        // Using managed delegate, the instance must be referenced to prevents GC

        #endregion Comments

#if NETSTANDARD
		private static readonly OnCompletionFn OnCompletionHandler = new OnCompletionFn(OnCompletionCallback);
		[AllowReversePInvokeCalls]
#else
        [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
#endif
        private unsafe static void OnCompletionCallback(IntPtr ctx, IntPtr client, TBPacket* packet, byte* result, uint result_len)
        {
            var request = IRequest.FromUserData(packet->user_data);
            if (request != null)
            {
                var span = result_len > 0 ? new ReadOnlySpan<byte>(result, (int)result_len) : ReadOnlySpan<byte>.Empty;
                request.Complete(packet->operation, packet->status, span);
            }

        }

        #endregion Static callback
    }
}
