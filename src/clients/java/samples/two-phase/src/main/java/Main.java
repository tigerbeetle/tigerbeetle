package com.tigerbeetle.samples;

import java.util.Arrays;
import java.util.UUID;
import java.math.BigInteger;

import com.tigerbeetle.*;
import static com.tigerbeetle.AssertionError.assertTrue;


public final class Main {

    public static void main(String[] args) throws Exception {
        String replicaAddress = System.getenv("TB_ADDRESS");

        byte[] clusterID = UInt128.asBytes(0);
        String replicaAddresses = replicaAddress == null ? "3000" : replicaAddress;
        try (var client = new Client(clusterID, replicaAddresses)) {
            // Create accounts.
            final var accountDrId = UInt128.id();
            final var accountCrId = UInt128.id();

            AccountBatch accounts = new AccountBatch(2);
            accounts.add();
            accounts.setId(accountDrId);
            accounts.setLedger(1);
            accounts.setCode(1);

            accounts.add();
            accounts.setId(accountCrId);
            accounts.setLedger(1);
            accounts.setCode(1);

            CreateAccountResultBatch accountResults = client.createAccounts(accounts);
            while (accountResults.next()) {
                switch (accountResults.getStatus()) {
                    case Created:
                        break;
                    default:
                        throw new Exception(String.format("Error creating account %d: %s\n",
                                accountResults.getPosition(),
                                accountResults.getStatus()));
                }
            }

            // Create pending transfer.
            TransferBatch transfers = new TransferBatch(1);
            final var transferPendingId = UInt128.id();
            transfers.add();
            transfers.setId(transferPendingId);
            transfers.setDebitAccountId(accountDrId);
            transfers.setCreditAccountId(accountCrId);
            transfers.setLedger(1);
            transfers.setCode(1);
            transfers.setAmount(500);
            transfers.setFlags(TransferFlags.PENDING);

            CreateTransferResultBatch transferResults = client.createTransfers(transfers);
            assertTrue(transferResults.getLength() == transfers.getLength());

            while (transferResults.next()) {
                switch (transferResults.getStatus()) {
                    case Created:
                        break;
                    default:
                        throw new Exception(String.format("Error creating transfer %d: %s\n",
                                transferResults.getPosition(),
                                transferResults.getStatus()));
                }
            }

            // Fetch and validate pending account balances.
            IdBatch ids = new IdBatch(2);
            ids.add(accountDrId);
            ids.add(accountCrId);
            accounts = client.lookupAccounts(ids);
            assertTrue(accounts.getLength() == 2);

            while (accounts.next()) {
                if (Arrays.equals(accounts.getId(), accountDrId)) {
                    assertTrue(accounts.getDebitsPosted().intValueExact() == 0);
                    assertTrue(accounts.getCreditsPosted().intValueExact() == 0);
                    assertTrue(accounts.getDebitsPending().intValueExact() == 500);
                    assertTrue(accounts.getCreditsPending().intValueExact() == 0);
                } else if (Arrays.equals(accounts.getId(), accountCrId)) {
                    assertTrue(accounts.getDebitsPosted().intValueExact() == 0);
                    assertTrue(accounts.getCreditsPosted().intValueExact() == 0);
                    assertTrue(accounts.getDebitsPending().intValueExact() == 0);
                    assertTrue(accounts.getCreditsPending().intValueExact() == 500);
                } else {
                    throw new Exception(String.format("Unexpected account: %s\n",
                            UInt128.asBigInteger(accounts.getId()).toString()));
                }
            }

            // Post pending transfer.
            transfers = new TransferBatch(1);
            transfers.add();
            transfers.setId(UInt128.id());
            transfers.setPendingId(transferPendingId);
            transfers.setDebitAccountId(accountDrId);
            transfers.setCreditAccountId(accountCrId);
            transfers.setLedger(1);
            transfers.setCode(1);
            // You can post any amount between zero and the pending transfer amount,
            // or void the two-phase transfer using the `VOID_PENDING_TRANSFER` flag.
            transfers.setAmount(500);
            transfers.setFlags(TransferFlags.POST_PENDING_TRANSFER);

            transferResults = client.createTransfers(transfers);
            assertTrue(transferResults.getLength() == transfers.getLength());

            while (transferResults.next()) {
                switch (transferResults.getStatus()) {
                    case Created:
                        break;
                    default:
                        throw new Exception(String.format("Error creating transfer %d: %s\n",
                                transferResults.getPosition(),
                                transferResults.getStatus()));
                }
            }

            // Fetch and validate final account balances.
            ids = new IdBatch(2);
            ids.add(accountDrId);
            ids.add(accountCrId);
            accounts = client.lookupAccounts(ids);
            assertTrue(accounts.getLength() == 2);

            while (accounts.next()) {
                if (Arrays.equals(accounts.getId(), accountDrId)) {
                    assertTrue(accounts.getDebitsPosted().intValueExact() == 500);
                    assertTrue(accounts.getCreditsPosted().intValueExact() == 0);
                    assertTrue(accounts.getDebitsPending().intValueExact() == 0);
                    assertTrue(accounts.getCreditsPending().intValueExact() == 0);
                } else if (Arrays.equals(accounts.getId(), accountCrId)) {
                    assertTrue(accounts.getDebitsPosted().intValueExact() == 0);
                    assertTrue(accounts.getCreditsPosted().intValueExact() == 500);
                    assertTrue(accounts.getDebitsPending().intValueExact() == 0);
                    assertTrue(accounts.getCreditsPending().intValueExact() == 0);
                } else {
                    throw new Exception(String.format("Unexpected account: %s\n",
                            UInt128.asBigInteger(accounts.getId()).toString()));
                }
            }
        }
    }
}

