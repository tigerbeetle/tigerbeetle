using System;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace TigerBeetle;

internal sealed class EchoClient : IDisposable
{
    private readonly NativeClient nativeClient;

    public EchoClient(UInt128 clusterID, string[] addresses)
    {
        this.nativeClient = NativeClient.InitEcho(clusterID, addresses);
    }

    public Account[] Echo(ReadOnlySpan<Account> batch)
    {
        return nativeClient.CallRequest<Account, Account>(TBOperation.CreateAccounts, batch);
    }

    public Task<Account[]> EchoAsync(ReadOnlyMemory<Account> batch)
    {
        return nativeClient.CallRequestAsync<Account, Account>(TBOperation.CreateAccounts, batch);
    }

    public Transfer[] Echo(ReadOnlySpan<Transfer> batch)
    {
        return nativeClient.CallRequest<Transfer, Transfer>(TBOperation.CreateTransfers, batch);
    }

    public Task<Transfer[]> EchoAsync(ReadOnlyMemory<Transfer> batch)
    {
        return nativeClient.CallRequestAsync<Transfer, Transfer>(TBOperation.CreateTransfers, batch);
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
}
