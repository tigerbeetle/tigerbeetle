using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

[assembly: InternalsVisibleTo("TigerBeetle.Tests")]

namespace TigerBeetle;

public sealed class Client : IDisposable
{
    private readonly UInt128 clusterID;
    private readonly NativeClient nativeClient;

    public Client(UInt128 clusterID, string[] addresses)
    {
        this.nativeClient = NativeClient.Init(clusterID, addresses);
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

    public CreateAccountsResult[] CreateAccounts(ReadOnlySpan<Account> batch)
    {
        return nativeClient.CallRequest<CreateAccountsResult, Account>(TBOperation.CreateAccounts, batch);
    }

    public Task<CreateAccountsResult[]> CreateAccountsAsync(ReadOnlyMemory<Account> batch)
    {
        return nativeClient.CallRequestAsync<CreateAccountsResult, Account>(TBOperation.CreateAccounts, batch);
    }

    public CreateTransfersResult[] CreateTransfers(ReadOnlySpan<Transfer> batch)
    {
        return nativeClient.CallRequest<CreateTransfersResult, Transfer>(TBOperation.CreateTransfers, batch);
    }

    public Task<CreateTransfersResult[]> CreateTransfersAsync(ReadOnlyMemory<Transfer> batch)
    {
        return nativeClient.CallRequestAsync<CreateTransfersResult, Transfer>(TBOperation.CreateTransfers, batch);
    }

    public Account[] LookupAccounts(ReadOnlySpan<UInt128> batch)
    {
        return nativeClient.CallRequest<Account, UInt128>(TBOperation.LookupAccounts, batch);
    }

    public Task<Account[]> LookupAccountsAsync(ReadOnlyMemory<UInt128> batch)
    {
        return nativeClient.CallRequestAsync<Account, UInt128>(TBOperation.LookupAccounts, batch);
    }

    public Transfer[] LookupTransfers(ReadOnlySpan<UInt128> batch)
    {
        return nativeClient.CallRequest<Transfer, UInt128>(TBOperation.LookupTransfers, batch);
    }

    public Task<Transfer[]> LookupTransfersAsync(ReadOnlyMemory<UInt128> batch)
    {
        return nativeClient.CallRequestAsync<Transfer, UInt128>(TBOperation.LookupTransfers, batch);
    }

    public Transfer[] GetAccountTransfers(AccountFilter filter)
    {
        return nativeClient.CallRequest<Transfer, AccountFilter>(TBOperation.GetAccountTransfers, new[] { filter });
    }

    public Task<Transfer[]> GetAccountTransfersAsync(AccountFilter filter)
    {
        return nativeClient.CallRequestAsync<Transfer, AccountFilter>(TBOperation.GetAccountTransfers, new[] { filter });
    }

    public AccountBalance[] GetAccountBalances(AccountFilter filter)
    {
        return nativeClient.CallRequest<AccountBalance, AccountFilter>(TBOperation.GetAccountBalances, new[] { filter });
    }

    public Task<AccountBalance[]> GetAccountBalancesAsync(AccountFilter filter)
    {
        return nativeClient.CallRequestAsync<AccountBalance, AccountFilter>(TBOperation.GetAccountBalances, new[] { filter });
    }

    public Account[] QueryAccounts(QueryFilter filter)
    {
        return nativeClient.CallRequest<Account, QueryFilter>(TBOperation.QueryAccounts, new[] { filter });
    }

    public Task<Account[]> QueryAccountsAsync(QueryFilter filter)
    {
        return nativeClient.CallRequestAsync<Account, QueryFilter>(TBOperation.QueryAccounts, new[] { filter });
    }

    public Transfer[] QueryTransfers(QueryFilter filter)
    {
        return nativeClient.CallRequest<Transfer, QueryFilter>(TBOperation.QueryTransfers, new[] { filter });
    }

    public Task<Transfer[]> QueryTransfersAsync(QueryFilter filter)
    {
        return nativeClient.CallRequestAsync<Transfer, QueryFilter>(TBOperation.QueryTransfers, new[] { filter });
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
