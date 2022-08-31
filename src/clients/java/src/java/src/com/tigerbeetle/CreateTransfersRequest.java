package com.tigerbeetle;

class CreateTransfersRequest extends Request<CreateTransfersResult> {

    protected CreateTransfersRequest(Client client, TransfersBatch batch) {
        super(client, Request.Operations.CREATE_TRANSFERS, batch);
    }

}
