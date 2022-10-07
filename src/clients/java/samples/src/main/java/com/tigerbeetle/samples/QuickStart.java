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
             * ################### Step 1 ###################
             */
            System.out.printf("Creating accounts ...%n");

            // Creating a batch of two accounts.
            final var accounts = new AccountBatch(2);

            // Adding account 1.
            accounts.add();

            final var customerId = UInt128.asBytes(UUID.randomUUID());
            accounts.setId(customerId);
            accounts.setUserData(null);
            accounts.setCode(AccountCodes.CUSTOMER.Code);
            accounts.setLedger(Ledgers.EUR.Code);
            accounts.setFlags(AccountFlags.NONE);

            // Adding account 2.
            accounts.add();

            final var supplierId = UInt128.asBytes(UUID.randomUUID());
            accounts.setId(supplierId);
            accounts.setUserData(null);
            accounts.setCode(AccountCodes.SUPPLIER.Code);
            accounts.setLedger(Ledgers.EUR.Code);
            accounts.setFlags(AccountFlags.NONE);

            // Submitting the accounts for creation.
            final var accountErrors = client.createAccounts(accounts);

            // Checking for errors during the creation.
            if (accountErrors.getLength() > 0) {

                while (accountErrors.next()) {

                    final var errorIndex = accountErrors.getIndex();
                    accounts.setPosition(errorIndex);

                    System.err.printf("Error creating account %s: %s.%n",
                            UInt128.asBigInteger(accounts.getId()), accountErrors.getResult());
                }

                return;
            }

            /*
             * ################### Step 2 ###################
             */
            System.out.printf("Creating a transfer ...%n");

            // Creating a batch of just one transfer.
            var transfers = new TransferBatch(1);

            // Example: The customer wants to pay EUR 99.00 to the supplier.
            transfers.add();
            transfers.setId(UInt128.asBytes(UUID.randomUUID()));
            transfers.setDebitAccountId(customerId);
            transfers.setCreditAccountId(supplierId);
            transfers.setCode(TransferCodes.PAYMENT.Code);
            transfers.setLedger(Ledgers.EUR.Code);
            transfers.setAmount(9900L);

            // Submitting the transfer
            var transferErrors = client.createTransfers(transfers);

            // Checking for any errors during the transfer.
            if (transferErrors.next()) {
                System.err.printf("Error creating the transfer: %s.%n", transferErrors.getResult());
                return;
            }

            /*
             * ################### Step 3 ###################
             */
            System.out.printf("Checking the account's balance after the transfer:%n");

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
