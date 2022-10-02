package com.tigerbeetle.samples;

/**
 * Example of account codes.
 */
public enum AccountCodes {
    CUSTOMER(1001),
    SUPPLIER(2001),
    AGENTS(3001);

    public final int Code;

    AccountCodes(int code) {
        Code = code;
    }

    public static AccountCodes fromCode(int code) {

        for (var accountCode : AccountCodes.values()) {
            if (accountCode.Code == code) {
                return accountCode;
            }
        }

        throw new IllegalArgumentException("Unknown account code");
    }
}
