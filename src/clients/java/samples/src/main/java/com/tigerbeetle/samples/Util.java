package com.tigerbeetle.samples;

import java.util.ArrayList;
import java.util.StringJoiner;
import com.tigerbeetle.AccountBatch;
import com.tigerbeetle.AccountFlags;
import com.tigerbeetle.UInt128;
import com.jakewharton.fliptables.FlipTable;

public final class Util {

    private final static String[] ACCOUNT_HEADERS = new String[] {"ID", "UserData", "Code",
            "Ledger", "Flags", "DebitsPosted", "DebitsPending", "CreditPosted", "CreditsPending"};

    private Util() {}

    /**
     * Prints a table of all accounts to the standard output.
     */
    public static void printAccounts(final AccountBatch batch) {

        ArrayList<String[]> data = new ArrayList<String[]>();

        batch.beforeFirst();

        while (batch.next()) {

            ArrayList<String> row = new ArrayList<String>();

            row.add(UInt128.asBigInteger(batch.getId()).toString());
            row.add(UInt128.asBigInteger(batch.getUserData()).toString());
            row.add(String.format("%d - %s", batch.getCode(),
                    AccountCodes.fromCode(batch.getCode())));
            row.add(String.format("%d - %s", batch.getLedger(),
                    Ledgers.fromCode(batch.getLedger())));


            var flags = new StringJoiner(System.lineSeparator());
            if (AccountFlags.hasCreditsMustNotExceedDebits(batch.getFlags()))
                flags.add("credits_must_not_exceed_debits");
            if (AccountFlags.hasDebitsMustNotExceedCredits(batch.getFlags()))
                flags.add("debits_must_not_exceed_credits");
            if (AccountFlags.hasLinked(batch.getFlags()))
                flags.add("linked");
            if (flags.length() == 0)
                flags.add("none");
            row.add(flags.toString());

            row.add(Long.toUnsignedString(batch.getDebitsPosted()));
            row.add(Long.toUnsignedString(batch.getDebitsPending()));
            row.add(Long.toUnsignedString(batch.getCreditsPosted()));
            row.add(Long.toUnsignedString(batch.getCreditsPending()));

            data.add(row.toArray(new String[0]));
        }

        final var table = FlipTable.of(ACCOUNT_HEADERS, data.toArray(new String[0][]));
        System.out.println(table);
    }
}
