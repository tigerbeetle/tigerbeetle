package com.tigerbeetle;

public interface AccountFlags {
    public static final short NONE = (short) 0;
    public static final short LINKED = (short) (1 << 0);
    public static final short DEBITS_MUST_NOT_EXCEED_CREDITS = (short) (1 << 1);
    public static final short CREDITS_MUST_NOT_EXCEED_DEBITS = (short) (1 << 2);
}
