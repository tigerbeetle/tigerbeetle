package com.tigerbeetle;

import java.util.StringJoiner;

public final class Client implements AutoCloseable {
    static {
        System.loadLibrary("tb_jniclient");
    }
  
    private static final int DEFAULT_MAX_CONCURRENCY = 32;

    private final int clusterID;
    private long clientHandle;
    private long packetsHead;
    private long packetsTail;

    public Client(int clusterID, String[] addresses, int maxConcurrency) throws IllegalArgumentException, Exception
    {
        if (clusterID < 0) throw new IllegalArgumentException("clusterID must be positive");
        if (addresses == null || addresses.length == 0) throw new IllegalArgumentException("Invalid addresses");

        // Cap the maximum amount of packets
        if (maxConcurrency <= 0) throw new IllegalArgumentException("Invalid maxConcurrency");
        if (maxConcurrency > 4096) maxConcurrency = 4096;

        var joiner = new StringJoiner(",");
        for(var address : addresses)
        {
            joiner.add(address);
        }

        this.clusterID = clusterID;
        int status = clientInit(clusterID, joiner.toString(), maxConcurrency);
        if (status != 0) throw new Exception("result " + status);
    }

    @Override
    public void close() throws Exception {
        if (clientHandle != 0)
        {
            clientDeinit(clientHandle);
            clientHandle = 0;
            packetsHead = 0;
            packetsTail = 0;
        }
    }

    private native int clientInit(int clusterID, String addresses, int maxConcurrency);
    private native void clientDeinit(long clientHandle);

    public CreateAccountResult CreateAccount(Account account)
    {
        var batch = new AccountsBatch(1);
        batch.Add(account);

        CreateAccountsResult[] results = CreateAccounts(batch);
        if (results.length == 0)
        {
            return CreateAccountResult.Ok;
        }
        else
        {
            return results[0].result;
        }
    }

    public CreateAccountsResult[] CreateAccounts(AccountsBatch batch)
    {
        // TODO:
        return null;
    }

    public CreateAccountsResult[] CreateAccounts(Account[] batch)
    {
        return CreateAccounts(new AccountsBatch(batch));
    }

    public static void main(String[] args) {
        try (var client = new Client(0, new String[] { "127.0.0.1:3001" }, 32)) {
            
            System.out.printf("Connected to cluster {}", client.clusterID);

        } catch (Exception e) {
            System.out.println("Big bad Zig error handled in Java >:(");
            e.printStackTrace();
        }
    }


}