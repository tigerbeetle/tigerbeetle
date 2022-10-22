package com.tigerbeetle.examples;

/**
 * Example of ledgers
 */
public enum Ledgers {

    /** US Dollar */
    USD(840),

    /** Euro */
    EUR(978),

    /** Pound sterling */
    GBP(826);

    public final int Code;

    Ledgers(int code) {
        Code = code;
    }

    public static Ledgers fromCode(int code) {

        for (var ledger : Ledgers.values()) {
            if (ledger.Code == code) {
                return ledger;
            }
        }

        throw new IllegalArgumentException("Unknown ledger");
    }

}
