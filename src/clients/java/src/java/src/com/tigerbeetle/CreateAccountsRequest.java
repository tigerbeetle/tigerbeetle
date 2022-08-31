package com.tigerbeetle;

class CreateAccountsRequest extends Request<CreateAccountsResult> {

    protected CreateAccountsRequest(Client client, AccountsBatch batch) {
        super(client, Request.Operations.CREATE_ACCOUNTS, batch);
    }

}
