package com.tigerbeetle;

public final class Client {
    static {
        System.loadLibrary("tb_jniclient");
    }
  
    public static native String greet(String name);

    public CreateAccountResult CreateAccount(Account account)
    {
        AccountsBatch batch = new AccountsBatch(1);
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
        try {
            System.out.println(greet("abc"));
        } catch (Exception e) {
            System.out.println("Big bad Zig error handled in Java >:(");
            e.printStackTrace();
        }
    }
}