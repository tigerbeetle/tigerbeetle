using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

[assembly: InternalsVisibleTo("TigerBeetle.Tests")]

namespace TigerBeetle;

public sealed class Client : IDisposable
{
    private const int DEFAULT_CONCURRENCY_MAX = 256; // arbitrary

    private readonly UInt128 clusterID;
    private readonly NativeClient nativeClient;

    public Client(UInt128 clusterID, string[] addresses, int concurrencyMax = DEFAULT_CONCURRENCY_MAX)
    {
        this.nativeClient = NativeClient.Init(clusterID, addresses, concurrencyMax);
        this.clusterID = clusterID;
    }

    ~Client()
    {
        // NativeClient can be null if the constructor threw an exception.
        if (nativeClient != null)
        {
            Dispose(disposing: false);
        }
    }

    public UInt128 ClusterID => clusterID;

    public CreateAccountResult CreateAccount(Account account)
    {
        var ret = nativeClient.CallRequest<CreateAccountsResult, Account>(TBOperation.CreateAccounts, new[] { account });
        return ret.Length == 0 ? CreateAccountResult.Ok : ret[0].Result;
    }

    public CreateAccountsResult[] CreateAccounts(Account[] batch)
    {
        return nativeClient.CallRequest<CreateAccountsResult, Account>(TBOperation.CreateAccounts, batch);
    }

    public Task<CreateAccountResult> CreateAccountAsync(Account account)
    {
        return nativeClient.CallRequestAsync<CreateAccountsResult, Account>(TBOperation.CreateAccounts, new[] { account })
        .ContinueWith(x => x.Result.Length == 0 ? CreateAccountResult.Ok : x.Result[0].Result);
    }

    public Task<CreateAccountsResult[]> CreateAccountsAsync(Account[] batch)
    {
        return nativeClient.CallRequestAsync<CreateAccountsResult, Account>(TBOperation.CreateAccounts, batch);
    }

    public CreateTransferResult CreateTransfer(Transfer transfer)
    {
        var ret = nativeClient.CallRequest<CreateTransfersResult, Transfer>(TBOperation.CreateTransfers, new[] { transfer });
        return ret.Length == 0 ? CreateTransferResult.Ok : ret[0].Result;
    }

    public CreateTransfersResult[] CreateTransfers(Transfer[] batch)
    {
        return nativeClient.CallRequest<CreateTransfersResult, Transfer>(TBOperation.CreateTransfers, batch);
    }

    public Task<CreateTransferResult> CreateTransferAsync(Transfer transfer)
    {
        return nativeClient.CallRequestAsync<CreateTransfersResult, Transfer>(TBOperation.CreateTransfers, new[] { transfer })
        .ContinueWith(x => x.Result.Length == 0 ? CreateTransferResult.Ok : x.Result[0].Result);
    }

    public Task<CreateTransfersResult[]> CreateTransfersAsync(Transfer[] batch)
    {
        return nativeClient.CallRequestAsync<CreateTransfersResult, Transfer>(TBOperation.CreateTransfers, batch);
    }

    public Account? LookupAccount(UInt128 id)
    {
        var ret = nativeClient.CallRequest<Account, UInt128>(TBOperation.LookupAccounts, new[] { id });
        return ret.Length == 0 ? null : ret[0];
    }

    public Account[] LookupAccounts(UInt128[] ids)
    {
        return nativeClient.CallRequest<Account, UInt128>(TBOperation.LookupAccounts, ids);
    }

    public Task<Account?> LookupAccountAsync(UInt128 id)
    {
        return nativeClient.CallRequestAsync<Account, UInt128>(TBOperation.LookupAccounts, new[] { id })
        .ContinueWith(x => x.Result.Length == 0 ? (Account?)null : x.Result[0]);
    }

    public Task<Account[]> LookupAccountsAsync(UInt128[] ids)
    {
        return nativeClient.CallRequestAsync<Account, UInt128>(TBOperation.LookupAccounts, ids);
    }

    public Transfer? LookupTransfer(UInt128 id)
    {
        var ret = nativeClient.CallRequest<Transfer, UInt128>(TBOperation.LookupTransfers, new[] { id });
        return ret.Length == 0 ? null : ret[0];
    }

    public Transfer[] LookupTransfers(UInt128[] ids)
    {
        return nativeClient.CallRequest<Transfer, UInt128>(TBOperation.LookupTransfers, ids);
    }

    public Task<Transfer?> LookupTransferAsync(UInt128 id)
    {
        return nativeClient.CallRequestAsync<Transfer, UInt128>(TBOperation.LookupTransfers, new[] { id })
        .ContinueWith(x => x.Result.Length == 0 ? (Transfer?)null : x.Result[0]);
    }

    public Task<Transfer[]> LookupTransfersAsync(UInt128[] ids)
    {
        return nativeClient.CallRequestAsync<Transfer, UInt128>(TBOperation.LookupTransfers, ids);
    }

    public Transfer[] GetAccountTransfers(GetAccountTransfers filter)
    {
        return nativeClient.CallRequest<Transfer, GetAccountTransfers>(TBOperation.GetAccountTransfers, new[] { filter });
    }

    public Task<Transfer[]> GetAccountTransfersAsync(GetAccountTransfers filter)
    {
        return nativeClient.CallRequestAsync<Transfer, GetAccountTransfers>(TBOperation.GetAccountTransfers, new[] { filter });
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
