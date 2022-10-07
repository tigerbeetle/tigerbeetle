package com.tigerbeetle.samples;

import java.util.UUID;
import com.tigerbeetle.*;

/**
 * A quick overview of TigerBeetle's operation creating two accounts and a single transfer between
 * them.
 */
public final class QuickStart {

    public static void main(String[] args) {

        final var cluster = Util.getCluster(args);
        final var addresses = Util.getAddresses(args);

        System.out.println("TigerBeetle QuickStart demo program");

        try (var client = new Client(cluster, addresses)) {

            /*
             * ## Step 1 - Creating accounts.
             */
            System.out.printf("Creating accounts ...%n");

            // Creating a batch of two accounts.
            final var accounts = new AccountBatch(2);

            // Adding account 1.
            accounts.add();

            final var customerId = UInt128.asBytes(1L);
            accounts.setId(customerId);
            accounts.setUserData(null);
            accounts.setCode(AccountCodes.CUSTOMER.Code);
            accounts.setLedger(Ledgers.EUR.Code);
            accounts.setFlags(AccountFlags.NONE);

            // Adding account 2.
            accounts.add();

            final var supplierId = UInt128.asBytes(2L);
            accounts.setId(supplierId);
            accounts.setUserData(null);
            accounts.setCode(AccountCodes.SUPPLIER.Code);
            accounts.setLedger(Ledgers.EUR.Code);
            accounts.setFlags(AccountFlags.NONE);

            // Submitting the accounts for creation.
            final var accountErrors = client.createAccounts(accounts);

            // Checking for errors during the creation.
            while (accountErrors.next()) {

                final var errorIndex = accountErrors.getIndex();
                accounts.setPosition(errorIndex);

                switch (accountErrors.getResult()) {

                    // Checking if TigerBeetle already has this account created.
                    // It's probably from the last time you ran this program, so it ok to
                    // continue.
                    case Exists:
                    case ExistsWithDifferentDebitsPosted:
                    case ExistsWithDifferentDebitsPending:
                    case ExistsWithDifferentCreditsPosted:
                    case ExistsWithDifferentCreditsPending:
                        System.out.printf("Account %s already exists.%n",
                                UInt128.asBigInteger(accounts.getId()));
                        break;

                    // For any other result, we abort.
                    default:
                        System.err.printf("Error creating account %s: %s%n.",
                                UInt128.asBigInteger(accounts.getId()), accountErrors.getResult());
                        return;
                }
            }

            /*
             * ## Step 2 - Creating a transfer.
             */
            System.out.printf("Creating transfer ...%n");

            // Creating a batch of just one transfer.
            var transfers = new TransferBatch(1);

            // Example: The customer wants to pay 99.00 EUR to the supplier.
            transfers.add();
            transfers.setId(UInt128.asBytes(UUID.randomUUID()));
            transfers.setDebitAccountId(customerId);
            transfers.setCreditAccountId(supplierId);
            transfers.setCode(TransactionCodes.PAYMENT.Code);
            transfers.setLedger(Ledgers.EUR.Code);
            transfers.setAmount(9900L);

            // Submitting the transaction
            var transferErrors = client.createTransfers(transfers);

            // Checking for any errors during the transfer.
            if (transferErrors.next()) {
                System.err.printf("Error creating the transfer: %s%n.", transferErrors.getResult());
                return;
            }

            /*
             * ## Step 3 - Checking the balance.
             */
            System.out.printf("Looking up accounts ...%n");

            // Creating a batch with the account's ids.
            final var ids = new IdBatch(2);
            ids.add(customerId);
            ids.add(supplierId);

            // Looking up the accounts.
            final var balances = client.lookupAccounts(ids);
            Util.printAccounts(balances);

        } catch (Exception exception) {
            System.err.printf("Unexpected exception %s%n", exception);
        }
    }
}
