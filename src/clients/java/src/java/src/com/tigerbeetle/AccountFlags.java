package com.tigerbeetle;

public enum AccountFlags {
    None(Values.NONE),
    Linked(Values.LINKED),
    DebitsMustNotExceedCredits(Values.DEBITS_MUST_NOT_EXCEED_CREDITS),
    CreditsMustNotExceedDebits(Values.CREDITS_MUST_NOT_EXCEED_DEBITS);

    private static class Values {
        public static final short NONE = (short) 0;
        public static final short LINKED = (short) (1 << 0);
        public static final short DEBITS_MUST_NOT_EXCEED_CREDITS = (short) (1 << 1);
        public static final short CREDITS_MUST_NOT_EXCEED_DEBITS = (short) (1 << 2);
    }

    public final short value;

    private AccountFlags(short value) {
        this.value = value;
    }

    public static AccountFlags fromValue(short value) {
        switch (value) {
            case Values.LINKED:
                return Linked;
            case Values.DEBITS_MUST_NOT_EXCEED_CREDITS:
                return DebitsMustNotExceedCredits;
            case Values.CREDITS_MUST_NOT_EXCEED_DEBITS:
                return CreditsMustNotExceedDebits;
            default:
                return None;
        }
    }
}
