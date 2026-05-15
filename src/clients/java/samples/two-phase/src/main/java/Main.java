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
        String[] replicaAddresses = new String[] {replicaAddress == null ? "3000" : replicaAddress};
        try (var client = new Client(clusterID, replicaAddresses)) {
            // Create two accounts
            AccountBatch accounts = new AccountBatch(2);
            accounts.add();
            accounts.setId(1);
            accounts.setLedger(1);
            accounts.setCode(1);

            accounts.add();
            accounts.setId(2);
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

            // Start a pending transfer
            TransferBatch transfers = new TransferBatch(1);
            transfers.add();
            transfers.setId(1);
            transfers.setDebitAccountId(1);
            transfers.setCreditAccountId(2);
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

            // Validate accounts pending and posted debits/credits before finishing the
            // two-phase transfer
            IdBatch ids = new IdBatch(2);
            ids.add(1);
            ids.add(2);
            accounts = client.lookupAccounts(ids);
            assertTrue(accounts.getLength() == 2);

            while (accounts.next()) {
                if (accounts.getId(UInt128.LeastSignificant) == 1
                        && accounts.getId(UInt128.MostSignificant) == 0) {
                    assertTrue(accounts.getDebitsPosted().intValueExact() == 0);
                    assertTrue(accounts.getCreditsPosted().intValueExact() == 0);
                    assertTrue(accounts.getDebitsPending().intValueExact() == 500);
                    assertTrue(accounts.getCreditsPending().intValueExact() == 0);
                } else if (accounts.getId(UInt128.LeastSignificant) == 2
                        && accounts.getId(UInt128.MostSignificant) == 0) {
                    assertTrue(accounts.getDebitsPosted().intValueExact() == 0);
                    assertTrue(accounts.getCreditsPosted().intValueExact() == 0);
                    assertTrue(accounts.getDebitsPending().intValueExact() == 0);
                    assertTrue(accounts.getCreditsPending().intValueExact() == 500);
                } else {
                    throw new Exception(String.format("Unexpected account: %s\n",
                            UInt128.asBigInteger(accounts.getId()).toString()));
                }
            }

            // Create a second transfer simply posting the first transfer
            transfers = new TransferBatch(1);
            transfers.add();
            transfers.setId(2);
            transfers.setPendingId(1);
            transfers.setDebitAccountId(1);
            transfers.setCreditAccountId(2);
            transfers.setLedger(1);
            transfers.setCode(1);
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

            // Validate accounts pending and posted debits/credits after finishing the
            // two-phase transfer
            ids = new IdBatch(2);
            ids.add(1);
            ids.add(2);
            accounts = client.lookupAccounts(ids);
            assertTrue(accounts.getLength() == 2);

            while (accounts.next()) {
                if (accounts.getId(UInt128.LeastSignificant) == 1
                        && accounts.getId(UInt128.MostSignificant) == 0) {
                    assertTrue(accounts.getDebitsPosted().intValueExact() == 500);
                    assertTrue(accounts.getCreditsPosted().intValueExact() == 0);
                    assertTrue(accounts.getDebitsPending().intValueExact() == 0);
                    assertTrue(accounts.getCreditsPending().intValueExact() == 0);
                } else if (accounts.getId(UInt128.LeastSignificant) == 2
                        && accounts.getId(UInt128.MostSignificant) == 0) {
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

