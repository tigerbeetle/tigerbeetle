package com.tigerbeetle.samples;

/**
 * Example of transaction codes
 */
public enum TransactionCodes {
    DEPOSIT(1001),
    PAYMENT(2001),
    FEE(3001);

    public final int Code;

    TransactionCodes(int code) {
        Code = code;
    }

    public static TransactionCodes fromCode(int code) {

        for (var transactionCode : TransactionCodes.values()) {
            if (transactionCode.Code == code) {
                return transactionCode;
            }
        }

        throw new IllegalArgumentException("Unknown transaction code");
    }
}
