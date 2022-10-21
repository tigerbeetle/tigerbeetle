package com.tigerbeetle;

import java.nio.ByteBuffer;

/**
 * A {@link Batch} of results returned from the {@link Client#createTransfers transfer creation}
 * operation.
 * <p>
 * Successfully executed operations return an empty batch whilst unsuccessful ones return a batch
 * with errors for only the ones that failed. This instance is always ready-only.
 */
public final class CreateTransferResultBatch extends Batch {

    interface Struct {

        int Index = 0;
        int Result = 4;

        int SIZE = 8;
    }

    static final CreateTransferResultBatch EMPTY = new CreateTransferResultBatch(0);

    CreateTransferResultBatch(final int capacity) {
        super(capacity, Struct.SIZE);
    }

    CreateTransferResultBatch(ByteBuffer buffer) {
        super(buffer, Struct.SIZE);
    }

    /**
     * Gets the {@link TransferBatch#getPosition position} of the related transfer in the submitted
     * batch.
     *
     * @return a zero-based index.
     */
    public int getIndex() {
        return getUInt32(at(Struct.Index));
    }

    /**
     * Get the error that occurred during the creation of the transfer
     *
     * @return see {@link CreateTransferResult}.
     */
    public CreateTransferResult getResult() {
        final var value = getUInt32(at(Struct.Result));
        return CreateTransferResult.fromValue(value);
    }
}
