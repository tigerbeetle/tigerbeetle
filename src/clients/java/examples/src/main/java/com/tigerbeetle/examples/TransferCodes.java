package com.tigerbeetle.examples;

/**
 * Example of transfer codes.
 */
public enum TransferCodes {
    DEPOSIT(1001),
    PAYMENT(2001),
    FEE(3001),
    WITHDRAW(9001);

    public final int Code;

    TransferCodes(int code) {
        Code = code;
    }

    public static TransferCodes fromCode(int code) {

        for (var transferCode : TransferCodes.values()) {
            if (transferCode.Code == code) {
                return transferCode;
            }
        }

        throw new IllegalArgumentException("Unknown transfer code");
    }
}
