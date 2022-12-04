package com.tigerbeetle;

import java.nio.ByteBuffer;

/**
 * A {@link Batch} of results returned from the {@link Client#createAccounts account creation}
 * operation.
 * <p>
 * Successfully executed operations return an empty batch whilst unsuccessful ones return a batch
 * with errors for only the ones that failed. This instance is always ready-only.
 */
public final class CreateAccountResultBatch extends Batch {

    interface Struct {

        int Index = 0;
        int Result = 4;

        int SIZE = 8;
    }

    static final CreateAccountResultBatch EMPTY = new CreateAccountResultBatch(0);

    CreateAccountResultBatch(final int capacity) {
        super(capacity, Struct.SIZE);
    }

    CreateAccountResultBatch(ByteBuffer buffer) {
        super(buffer, Struct.SIZE);
    }

    /**
     * Gets the {@link AccountBatch#getPosition position} of the related account in the submitted
     * batch.
     *
     * @return a zero-based index.
     */
    public int getIndex() {
        return getUInt32(at(Struct.Index));
    }

    /**
     * Get the error that occurred during the creation of the account
     *
     * @return see {@link CreateAccountResult}.
     */
    public CreateAccountResult getResult() {
        final var value = getUInt32(at(Struct.Result));
        return CreateAccountResult.fromValue(value);
    }
}
