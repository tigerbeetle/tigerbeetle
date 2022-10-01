package com.tigerbeetle;

public interface AccountFlags {
    short NONE = (short) 0;
    short LINKED = (short) (1 << 0);
    short DEBITS_MUST_NOT_EXCEED_CREDITS = (short) (1 << 1);
    short CREDITS_MUST_NOT_EXCEED_DEBITS = (short) (1 << 2);
}
