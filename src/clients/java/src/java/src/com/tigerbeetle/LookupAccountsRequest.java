package com.tigerbeetle;

class LookupAccountsRequest extends Request<Account> {

    protected LookupAccountsRequest(Client client, UUIDsBatch batch)
            throws IllegalArgumentException {
        super(client, Request.Operations.LOOKUP_ACCOUNTS, batch);
    }

}
