using System;
using System.Collections.Generic;
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

        private const int DEFAULT_MAX_CONCURRENCY = 32;

        private readonly uint clusterID;
        private readonly NativeClient nativeClient;

        #endregion Fields

        #region Constructor

        public Client(uint clusterID, int[] replicaPorts, int maxConcurrency = DEFAULT_MAX_CONCURRENCY)
        : this(clusterID, replicaPorts.Select(x => x.ToString()), maxConcurrency)
        {
        }

        public Client(uint clusterID, string[] replicaAddresses, int maxConcurrency = DEFAULT_MAX_CONCURRENCY)
        : this(clusterID, replicaAddresses.Select(x => x), maxConcurrency)
        {
        }

        public Client(uint clusterID, IPEndPoint[] replicaEndpoints, int maxConcurrency = DEFAULT_MAX_CONCURRENCY)
        : this(clusterID, replicaEndpoints.Select(x => x.ToString()), maxConcurrency)
        {
        }

        private Client(uint clusterID, IEnumerable<string> configuration, int maxConcurrency)
        {
            if (configuration == null || !configuration.Any()) throw new ArgumentException("Invalid replica addresses");
            if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency));

            this.clusterID = clusterID;
            this.nativeClient = NativeClient.init(clusterID, string.Join(',', configuration), maxConcurrency);
        }

        ~Client()
        {
            Dispose(disposing: false);
        }

        #endregion Constructor

        #region Properties

        public uint ClusterID => clusterID;

        #endregion Properties

        #region Methods

        public CreateAccountResult CreateAccount(Account account)
        {
            var ret = CallRequest<CreateAccountsResult, Account>(TBOperation.CreateAccounts, new[] { account });
            return ret.Length == 0 ? CreateAccountResult.Ok : ret[0].Result;
        }

        public CreateAccountsResult[] CreateAccounts(Account[] batch)
        {
            return CallRequest<CreateAccountsResult, Account>(TBOperation.CreateAccounts, batch);
        }

        public Task<CreateAccountResult> CreateAccountAsync(Account account)
        {
            return CallRequestAsync<CreateAccountsResult, Account>(TBOperation.CreateAccounts, new[] { account })
            .ContinueWith(x => x.Result.Length == 0 ? CreateAccountResult.Ok : x.Result[0].Result);
        }

        public Task<CreateAccountsResult[]> CreateAccountsAsync(Account[] batch)
        {
            return CallRequestAsync<CreateAccountsResult, Account>(TBOperation.CreateAccounts, batch);
        }

        public CreateTransferResult CreateTransfer(Transfer transfer)
        {
            var ret = CallRequest<CreateTransfersResult, Transfer>(TBOperation.CreateTransfers, new[] { transfer });
            return ret.Length == 0 ? CreateTransferResult.Ok : ret[0].Result;
        }

        public CreateTransfersResult[] CreateTransfers(Transfer[] batch)
        {
            return CallRequest<CreateTransfersResult, Transfer>(TBOperation.CreateTransfers, batch);
        }

        public Task<CreateTransferResult> CreateTransferAsync(Transfer transfer)
        {
            return CallRequestAsync<CreateTransfersResult, Transfer>(TBOperation.CreateTransfers, new[] { transfer })
            .ContinueWith(x => x.Result.Length == 0 ? CreateTransferResult.Ok : x.Result[0].Result);
        }

        public Task<CreateTransfersResult[]> CreateTransfersAsync(Transfer[] batch)
        {
            return CallRequestAsync<CreateTransfersResult, Transfer>(TBOperation.CreateTransfers, batch);
        }

        public Account? LookupAccount(UInt128 id)
        {
            var ret = CallRequest<Account, UInt128>(TBOperation.LookupAccounts, new[] { id });
            return ret.Length == 0 ? null : ret[0];
        }

        public Account[] LookupAccounts(UInt128[] ids)
        {
            return CallRequest<Account, UInt128>(TBOperation.LookupAccounts, ids);
        }

        public Task<Account?> LookupAccountAsync(UInt128 id)
        {
            return CallRequestAsync<Account, UInt128>(TBOperation.LookupAccounts, new[] { id })
            .ContinueWith(x => x.Result.Length == 0 ? (Account?)null : x.Result[0]);
        }

        public Task<Account[]> LookupAccountsAsync(UInt128[] ids)
        {
            return CallRequestAsync<Account, UInt128>(TBOperation.LookupAccounts, ids);
        }

        public Transfer? LookupTransfer(UInt128 id)
        {
            var ret = CallRequest<Transfer, UInt128>(TBOperation.LookupTransfers, new[] { id });
            return ret.Length == 0 ? null : ret[0];
        }

        public Transfer[] LookupTransfers(UInt128[] ids)
        {
            return CallRequest<Transfer, UInt128>(TBOperation.LookupTransfers, ids);
        }

        public Task<Transfer?> LookupTransferAsync(UInt128 id)
        {
            return CallRequestAsync<Transfer, UInt128>(TBOperation.LookupTransfers, new[] { id })
            .ContinueWith(x => x.Result.Length == 0 ? (Transfer?)null : x.Result[0]);
        }

        public Task<Transfer[]> LookupTransfersAsync(UInt128[] ids)
        {
            return CallRequestAsync<Transfer, UInt128>(TBOperation.LookupTransfers, ids);
        }

        private TResult[] CallRequest<TResult, TBody>(TBOperation operation, TBody[] batch)
            where TResult : unmanaged
            where TBody : unmanaged
        {
            var packet = nativeClient.Rent();
            var blockingRequest = new BlockingRequest<TResult, TBody>(this.nativeClient, packet);

            blockingRequest.Submit(operation, batch);
            return blockingRequest.Wait();
        }

        private async Task<TResult[]> CallRequestAsync<TResult, TBody>(TBOperation operation, TBody[] batch)
            where TResult : unmanaged
            where TBody : unmanaged
        {
            var packet = await nativeClient.RentAsync();
            var asyncRequest = new AsyncRequest<TResult, TBody>(this.nativeClient, packet);

            asyncRequest.Submit(operation, batch);
            return await asyncRequest.Wait().ConfigureAwait(continueOnCapturedContext: false);
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            Dispose(disposing: true);
        }

        private void Dispose(bool disposing)
        {
            _ = disposing;
            nativeClient.Dispose();
        }

        #endregion Methods
    }
}