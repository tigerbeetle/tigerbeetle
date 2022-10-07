package com.tigerbeetle.examples;

import java.util.UUID;
import com.tigerbeetle.*;

/**
 * Demonstrates how to create groups of linked accounts and transfers that succeed or fail
 * atomically.
 */
public final class LinkedEvents {

    public static void main(String[] args) {

        final var cluster = Util.getCluster(args);
        final var addresses = Util.getAddresses(args);

        System.out.println("TigerBeetle LinkedEvents demo program");

        try (var client = new Client(cluster, addresses)) {

            /*
             * ################### Step 1 ###################
             */
            System.out.printf("Creating accounts ...%n");

            // Creating a batch of accounts.
            final var accounts = new AccountBatch(3);

            // Adding the first account.
            // https://docs.tigerbeetle.com/reference/accounts#flagslinked
            accounts.add();

            final var bankReserve = UInt128.asBytes(UUID.randomUUID());
            accounts.setId(bankReserve);
            accounts.setUserData(null);
            accounts.setCode(AccountCodes.BANK_RESERVE.Code);
            accounts.setLedger(Ledgers.USD.Code);
            accounts.setFlags(AccountFlags.LINKED);

            // Adding another account, linked to the previous one.
            accounts.add();

            final var customerId = UInt128.asBytes(UUID.randomUUID());
            accounts.setId(customerId);
            accounts.setUserData(null);
            accounts.setCode(AccountCodes.CUSTOMER.Code);
            accounts.setLedger(Ledgers.USD.Code);
            accounts.setFlags(AccountFlags.LINKED | AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS);

            // Adding the last account, closing the linked chain.
            accounts.add();

            final var supplierId = UInt128.asBytes(UUID.randomUUID());
            accounts.setId(supplierId);
            accounts.setUserData(null);
            accounts.setCode(AccountCodes.SUPPLIER.Code);
            accounts.setLedger(Ledgers.USD.Code);
            accounts.setFlags(AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS); // The last event in a
                                                                            // chain must not have
                                                                            // the LINKED flag,
                                                                            // closing the chain.

            // Submitting the accounts for creation.
            final var accountErrors = client.createAccounts(accounts);

            // Checking for errors during the creation.
            // As the accounts are linked, all must succeed together.
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
            System.out.printf("Creating linked transfers ...%n");

            // Creating a batch of transfers.
            // See: https://docs.tigerbeetle.com/reference/transfers/#flagslinked
            var transfers = new TransferBatch(2);

            // Example: The customer wants to pay USD 99.00 to the supplier.
            transfers.add();
            final var paymentId = UInt128.asBytes(UUID.randomUUID());
            transfers.setId(paymentId);
            transfers.setDebitAccountId(customerId);
            transfers.setCreditAccountId(supplierId);
            transfers.setCode(TransferCodes.PAYMENT.Code);
            transfers.setLedger(Ledgers.USD.Code);
            transfers.setAmount(9900L);
            transfers.setFlags(TransferFlags.LINKED);

            // Example: The supplier pays USD 0.99 of the fee.
            transfers.add();
            final var feeId = UInt128.asBytes(UUID.randomUUID());
            transfers.setId(feeId);
            transfers.setDebitAccountId(supplierId);
            transfers.setCreditAccountId(bankReserve);
            transfers.setCode(TransferCodes.FEE.Code);
            transfers.setLedger(Ledgers.USD.Code);
            transfers.setAmount(99L);
            transfers.setFlags(TransferFlags.NONE); // The last event in a
                                                    // chain must not have
                                                    // the LINKED flag,
                                                    // closing the chain.

            // Submitting the transfers
            var transferErrors = client.createTransfers(transfers);

            // We expect errors: the customer's account had insufficient balance for this
            // transfer, so both linked transfers must fail.
            // See:
            // https://docs.tigerbeetle.com/reference/accounts#flagsdebits_must_not_exceed_credits
            while (transferErrors.next()) {

                System.out.printf("Error creating the transfer %d: %s.%n",
                        transferErrors.getIndex(), transferErrors.getResult());
            }

            /*
             * ################### Step 3 ###################
             */
            System.out.printf("Checking the account's balance after the failed transfers:%n");

            // Creating a batch with the account's ids.
            var ids = new IdBatch(2);
            ids.add(customerId);
            ids.add(supplierId);

            // Looking up the accounts.
            var balances = client.lookupAccounts(ids);
            Util.printAccounts(balances);

            /*
             * ################### Step 4 ###################
             */
            System.out.printf("Making a deposit and submitting the linked transfers again ...%n");

            // Creating a batch of transfers.
            transfers = new TransferBatch(3);

            // Example: The customer receives a USD 500.00 deposit.
            transfers.add();
            final var depositId = UInt128.asBytes(UUID.randomUUID());
            transfers.setId(depositId);
            transfers.setDebitAccountId(bankReserve);
            transfers.setCreditAccountId(customerId);
            transfers.setCode(TransferCodes.DEPOSIT.Code);
            transfers.setLedger(Ledgers.USD.Code);
            transfers.setAmount(50000L);
            transfers.setFlags(TransferFlags.NONE); // This transfer is not part of the linked
                                                    // chain.

            // Example: Now, the customer has sufficient credit to pay USD 99.00 to the supplier.
            transfers.add();
            transfers.setId(paymentId);
            transfers.setDebitAccountId(customerId);
            transfers.setCreditAccountId(supplierId);
            transfers.setCode(TransferCodes.PAYMENT.Code);
            transfers.setLedger(Ledgers.USD.Code);
            transfers.setAmount(9900L);
            transfers.setFlags(TransferFlags.LINKED);

            // Example: The supplier pays USD 0.99 of the fee.
            transfers.add();
            transfers.setId(feeId);
            transfers.setDebitAccountId(supplierId);
            transfers.setCreditAccountId(bankReserve);
            transfers.setCode(TransferCodes.FEE.Code);
            transfers.setLedger(Ledgers.USD.Code);
            transfers.setAmount(99L);
            transfers.setFlags(TransferFlags.NONE);

            // Submitting the transfers
            transferErrors = client.createTransfers(transfers);

            if (transferErrors.getLength() > 0) {

                while (transferErrors.next()) {
                    final var errorIndex = transferErrors.getIndex();
                    transfers.setPosition(errorIndex);
                    System.err.printf("Error creating the transfers %s: %s.%n",
                            UInt128.asBigInteger(transfers.getId()), transferErrors.getResult());
                }

                // We are not expecting any error this time.
                return;
            }

            /*
             * ################### Step 5 ###################
             */
            System.out.printf("Checking the account's balance after the succeeded transfers:%n");

            // Creating a batch with the account's ids.
            ids = new IdBatch(3);
            ids.add(bankReserve);
            ids.add(customerId);
            ids.add(supplierId);

            // Looking up the accounts.
            balances = client.lookupAccounts(ids);
            Util.printAccounts(balances);

        } catch (Exception exception) {
            System.err.printf("Unexpected exception %s%n", exception);
        }
    }
}
