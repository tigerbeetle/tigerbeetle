package com.tigerbeetle;

import java.nio.ByteBuffer;

public final class CreateAccountsResult {

    interface Struct {
        public static final int SIZE = 8;
    }

    public final int index;
    public final CreateAccountResult result;

    CreateAccountsResult(ByteBuffer ptr) {
        index = ptr.getInt();
        result = CreateAccountResult.fromValue(ptr.getInt());
    }

    CreateAccountsResult(int index, CreateAccountResult result) {
        this.index = index;
        this.result = result;
    }
}
