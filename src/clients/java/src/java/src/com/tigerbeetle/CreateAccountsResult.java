package com.tigerbeetle;

public final class CreateAccountsResult {
    
    public final int index;
    public final CreateAccountResult result;

    public CreateAccountsResult(int index, CreateAccountResult result)
    {
        this.index = index;
        this.result = result;
    }
}
