package com.tigerbeetle.samples;

import java.math.BigInteger;
import java.util.ArrayList;
import com.tigerbeetle.AccountBatch;
import com.tigerbeetle.UInt128;
import com.jakewharton.fliptables.FlipTable;

public final class Util {

    private final static String[] ACCOUNT_HEADERS = new String[] {"ID", "UserData", "Code",
            "Ledger", "DebitsPosted", "DebitsPending", "CreditPosted", "CreditsPending"};

    private Util() {}

    /**
     * Prints a table of all accounts to the standard output.
     */
    public static void printAccounts(final AccountBatch batch) {

        ArrayList<String[]> data = new ArrayList<String[]>();

        batch.beforeFirst();

        while (batch.next()) {

            ArrayList<String> row = new ArrayList<String>();

            row.add(toBigInteger(batch.getId()).toString());
            row.add(toBigInteger(batch.getUserData()).toString());
            row.add(String.format("%d - %s", batch.getCode(),
                    AccountCodes.fromCode(batch.getCode())));
            row.add(String.format("%d - %s", batch.getLedger(),
                    Ledgers.fromCode(batch.getLedger())));
            row.add(Long.toUnsignedString(batch.getDebitsPosted()));
            row.add(Long.toUnsignedString(batch.getDebitsPending()));
            row.add(Long.toUnsignedString(batch.getCreditsPosted()));
            row.add(Long.toUnsignedString(batch.getCreditsPending()));

            data.add(row.toArray(new String[0]));
        }

        final var table = FlipTable.of(ACCOUNT_HEADERS, data.toArray(new String[0][]));
        System.out.println(table);
    }


    /**
     * Naive conversion from an array of 16 bytes to a BigInteger
     */
    public static BigInteger toBigInteger(byte[] uint128) {

        final long leastSignificant = UInt128.asLong(uint128, UInt128.LeastSignificant);
        final long mostSignificant = UInt128.asLong(uint128, UInt128.MostSignificant);

        // BigInteger is stored as BIG ENDIAN
        return new BigInteger(Long.toBinaryString(mostSignificant), 2).shiftLeft(64)
                .add(new BigInteger(Long.toBinaryString(leastSignificant), 2));
    }

}
