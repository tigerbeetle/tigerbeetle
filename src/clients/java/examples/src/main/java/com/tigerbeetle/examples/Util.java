package com.tigerbeetle.examples;

import java.util.ArrayList;
import java.util.StringJoiner;
import com.tigerbeetle.AccountBatch;
import com.tigerbeetle.AccountFlags;
import com.tigerbeetle.UInt128;
import com.jakewharton.fliptables.FlipTable;

public final class Util {


    private final static int DEFAULT_CLUSTER_ID = 0;

    private final static String[] DEFAULT_CLUSTER_ADDRESSES = new String[] {"127.0.0.1:3000"};

    private final static String[] ACCOUNT_HEADERS = new String[] {"ID", "UserData", "Code",
            "Ledger", "Flags", "DebitsPosted", "DebitsPending", "CreditPosted", "CreditsPending"};

    private Util() {}

    public static int getCluster(final String[] args) {

        final var arg = getCommandLineArg("cluster", args);
        if (arg != null) {

            try {
                return Integer.parseInt(arg);
            } catch (NumberFormatException e) {
                // Ignore invalid integers
            }
        }

        return DEFAULT_CLUSTER_ID;
    }

    public static String[] getAddresses(final String[] args) {

        final var arg = getCommandLineArg("addresses", args);
        if (arg != null) {
            return arg.split(",");
        }

        return DEFAULT_CLUSTER_ADDRESSES;
    }

    private static String getCommandLineArg(final String key, final String[] args) {

        if (args != null) {

            for (final var arg : args) {
                final var parts = arg.split("=");
                if (parts != null && parts.length > 1) {
                    if (key.equals(parts[0])) {
                        return parts[1];
                    }
                }
            }
        }

        return null;
    }

    /**
     * Prints a table of all accounts to the standard output.
     */
    public static void printAccounts(final AccountBatch batch) {

        ArrayList<String[]> data = new ArrayList<String[]>();

        batch.beforeFirst();

        while (batch.next()) {

            ArrayList<String> row = new ArrayList<String>();

            row.add(idToString(batch.getId()));
            row.add(idToString(batch.getUserData()));
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

    private static String idToString(final byte[] id) {

        final int MAX_LEN = 6;
        final var str = UInt128.asBigInteger(id).toString();
        if (str.length() <= MAX_LEN) {
            return str;
        } else {
            return String.format("***%s", str.substring(str.length() - MAX_LEN, str.length() - 1));
        }
    }
}
