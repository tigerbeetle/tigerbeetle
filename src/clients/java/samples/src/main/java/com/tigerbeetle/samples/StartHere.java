package com.tigerbeetle.samples;

import java.util.UUID;
import com.tigerbeetle.*;

/**
 * A basic overview of TigerBeetle's operation creating two accounts and a single transfer between
 * them.
 */
public final class StartHere {

    public static void main(String[] args) {

        try (var client = new Client(0, new String[] {"3001"})) {

            // Creating two accounts
            final var accounts = new AccountBatch(2);

            // Adding a customer account
            final var customerId = UInt128.asBytes(UUID.randomUUID());
            accounts.add();
            accounts.setId(customerId);
            accounts.setCode(AccountCodes.CUSTOMER.Code);
            accounts.setLedger(Ledgers.EUR.Code);

            // Adding another account, a supplier
            final var supplierId = UInt128.asBytes(UUID.randomUUID());
            accounts.add();
            accounts.setId(supplierId);
            accounts.setCode(AccountCodes.SUPPLIER.Code);
            accounts.setLedger(Ledgers.EUR.Code);

            // Submitting the accounts
            final var accountErrors = client.createAccounts(accounts);

            // Checking for any errors during the creation
            if (accountErrors.getLength() > 0) {
                System.err.println("Error creating the accounts\n.");
                return;
            }

            // Creating just one transfer
            var transfers = new TransferBatch(1);

            // The customer wants to pay 99.00 EUR to the supplier.
            transfers.add();
            transfers.setId(UInt128.asBytes(UUID.randomUUID()));
            transfers.setDebitAccountId(customerId);
            transfers.setCreditAccountId(supplierId);
            transfers.setCode(TransactionCodes.PAYMENT.Code);
            transfers.setLedger(Ledgers.EUR.Code);
            transfers.setAmount(9900L);

            // Submitting the transaction
            var transferErrors = client.createTransfers(transfers);

            // Checking for any errors during the transfer
            if (transferErrors.getLength() > 0) {
                System.err.println("Error creating the transfer\n.");
                return;
            }

            // Now checking the accounts balance
            var ids = new IdBatch(2);
            ids.add(customerId);
            ids.add(supplierId);

            var balances = client.lookupAccounts(ids);

            while (balances.next()) {
                System.out.printf("Account ID: %s\n", UInt128.asUUID(balances.getId()));
                System.out.printf("Code: %d - %s\n", balances.getCode(),
                        AccountCodes.fromCode(balances.getCode()));
                System.out.printf("Ledger: %d - %s\n", balances.getLedger(),
                        Ledgers.fromCode(balances.getLedger()));
                System.out.printf("Credits posted: %d\n", balances.getCreditsPosted());
                System.out.printf("Debits posted: %d\n", balances.getDebitsPosted());
                System.out.println("---------------------------------------------");
            }

        } catch (Exception exception) {
            System.err.printf("Unexpected exception %s\n", exception);
        }
    }
}
