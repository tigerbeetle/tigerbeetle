using System;
using System.Threading.Tasks;

namespace TigerBeetle
{
    internal sealed class EchoClient : IDisposable
    {
        #region Fields

        private readonly NativeClient nativeClient;

        #endregion Fields

        #region Constructor

        public EchoClient(uint clusterID, string addresses, int maxConcurrency)
        {
            if (addresses == null) throw new ArgumentNullException(nameof(addresses));
            if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency));

            this.nativeClient = NativeClient.initEcho(clusterID, addresses, maxConcurrency);
        }

        ~EchoClient()
        {
            Dispose(disposing: false);
        }

        #endregion Constructor

        #region Methods

        public Account[] Echo(Account[] batch)
        {
            return CallRequest<Account, Account>(TBOperation.CreateAccounts, batch);
        }

        public Task<Account[]> EchoAsync(Account[] batch)
        {
            return CallRequestAsync<Account, Account>(TBOperation.CreateAccounts, batch);
        }

        public Transfer[] Echo(Transfer[] batch)
        {
            return CallRequest<Transfer, Transfer>(TBOperation.CreateTransfers, batch);
        }

        public Task<Transfer[]> EchoAsync(Transfer[] batch)
        {
            return CallRequestAsync<Transfer, Transfer>(TBOperation.CreateTransfers, batch);
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