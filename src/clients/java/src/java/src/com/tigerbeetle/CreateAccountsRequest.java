package com.tigerbeetle;

class CreateAccountsRequest extends Request<CreateAccountsResult> {

    protected CreateAccountsRequest(Client client, AccountsBatch batch) throws IllegalArgumentException {
        super(client, Request.Operations.CREATE_ACCOUNTS, batch);
    }

}
