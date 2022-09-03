package com.tigerbeetle;

class LookupTransfersRequest extends Request<Transfer> {

    protected LookupTransfersRequest(Client client, UUIDsBatch batch)
            throws IllegalArgumentException {
        super(client, Request.Operations.LOOKUP_TRANSFERS, batch);
    }

}
