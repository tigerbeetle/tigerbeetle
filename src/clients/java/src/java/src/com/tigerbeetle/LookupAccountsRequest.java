package com.tigerbeetle;

class LookupAccountsRequest extends Request<Account> {

    protected LookupAccountsRequest(Client client, UUIDsBatch batch) {
        super(client, Request.Operations.LOOKUP_ACCOUNTS, batch);
    }

}
