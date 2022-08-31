package com.tigerbeetle;

public final class CreateTransfersResult {

    public final int index;
    public final CreateTransferResult result;

    CreateTransfersResult(int index, CreateTransferResult result) {
        this.index = index;
        this.result = result;
    }
}
