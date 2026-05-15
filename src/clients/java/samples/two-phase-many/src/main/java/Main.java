package com.tigerbeetle.samples;

import java.util.Arrays;
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

            // Start five pending transfer.
            TransferBatch transfers = new TransferBatch(5);
            transfers.add();
            transfers.setId(1);
            transfers.setDebitAccountId(1);
            transfers.setCreditAccountId(2);
            transfers.setLedger(1);
            transfers.setCode(1);
            transfers.setAmount(100);
            transfers.setFlags(TransferFlags.PENDING);

            transfers.add();
            transfers.setId(2);
            transfers.setDebitAccountId(1);
            transfers.setCreditAccountId(2);
            transfers.setLedger(1);
            transfers.setCode(1);
            transfers.setAmount(200);
            transfers.setFlags(TransferFlags.PENDING);

            transfers.add();
            transfers.setId(3);
            transfers.setDebitAccountId(1);
            transfers.setCreditAccountId(2);
            transfers.setLedger(1);
            transfers.setCode(1);
            transfers.setAmount(300);
            transfers.setFlags(TransferFlags.PENDING);

            transfers.add();
            transfers.setId(4);
            transfers.setDebitAccountId(1);
            transfers.setCreditAccountId(2);
            transfers.setLedger(1);
            transfers.setCode(1);
            transfers.setAmount(400);
            transfers.setFlags(TransferFlags.PENDING);

            transfers.add();
            transfers.setId(5);
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

            // Validate accounts pending and posted debits/credits
            // before finishing the two-phase transfer.
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
                    assertTrue(accounts.getDebitsPending().intValueExact() == 1500);
                    assertTrue(accounts.getCreditsPending().intValueExact() == 0);
                } else if (Arrays.equals(accounts.getId(), UInt128.asBytes(2))) {
                    assertTrue(accounts.getDebitsPosted().intValueExact() == 0);
                    assertTrue(accounts.getCreditsPosted().intValueExact() == 0);
                    assertTrue(accounts.getDebitsPending().intValueExact() == 0);
                    assertTrue(accounts.getCreditsPending().intValueExact() == 1500);
                } else {
                    throw new Exception(String.format("Unexpected account: %s\n",
                            UInt128.asBigInteger(accounts.getId()).toString()));
                }
            }

            // Create a 6th transfer posting the 1st transfer.
            transfers = new TransferBatch(1);
            transfers.add();
            transfers.setId(6);
            transfers.setPendingId(1);
            transfers.setDebitAccountId(1);
            transfers.setCreditAccountId(2);
            transfers.setLedger(1);
            transfers.setCode(1);
            transfers.setAmount(100);
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

            // Validate account balances after posting 1st pending transfer.
            accounts = client.lookupAccounts(ids);
            assertTrue(accounts.getLength() == 2);

            while (accounts.next()) {
                if (accounts.getId(UInt128.LeastSignificant) == 1
                        && accounts.getId(UInt128.MostSignificant) == 0) {
                    assertTrue(accounts.getDebitsPosted().intValueExact() == 100);
                    assertTrue(accounts.getCreditsPosted().intValueExact() == 0);
                    assertTrue(accounts.getDebitsPending().intValueExact() == 1400);
                    assertTrue(accounts.getCreditsPending().intValueExact() == 0);
                } else if (accounts.getId(UInt128.LeastSignificant) == 2
                        && accounts.getId(UInt128.MostSignificant) == 0) {
                    assertTrue(accounts.getDebitsPosted().intValueExact() == 0);
                    assertTrue(accounts.getCreditsPosted().intValueExact() == 100);
                    assertTrue(accounts.getDebitsPending().intValueExact() == 0);
                    assertTrue(accounts.getCreditsPending().intValueExact() == 1400);
                } else {
                    throw new Exception(String.format("Unexpected account: %s\n",
                            UInt128.asBigInteger(accounts.getId()).toString()));
                }
            }

            // Create a 6th transfer voiding the 2nd transfer.
            transfers = new TransferBatch(1);
            transfers.add();
            transfers.setId(7);
            transfers.setPendingId(2);
            transfers.setDebitAccountId(1);
            transfers.setCreditAccountId(2);
            transfers.setLedger(1);
            transfers.setCode(1);
            transfers.setAmount(200);
            transfers.setFlags(TransferFlags.VOID_PENDING_TRANSFER);

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

            // Validate account balances after voiding 2nd pending transfer.
            accounts = client.lookupAccounts(ids);
            assertTrue(accounts.getLength() == 2);

            while (accounts.next()) {
                if (accounts.getId(UInt128.LeastSignificant) == 1
                        && accounts.getId(UInt128.MostSignificant) == 0) {
                    assertTrue(accounts.getDebitsPosted().intValueExact() == 100);
                    assertTrue(accounts.getCreditsPosted().intValueExact() == 0);
                    assertTrue(accounts.getDebitsPending().intValueExact() == 1200);
                    assertTrue(accounts.getCreditsPending().intValueExact() == 0);
                } else if (accounts.getId(UInt128.LeastSignificant) == 2
                        && accounts.getId(UInt128.MostSignificant) == 0) {
                    assertTrue(accounts.getDebitsPosted().intValueExact() == 0);
                    assertTrue(accounts.getCreditsPosted().intValueExact() == 100);
                    assertTrue(accounts.getDebitsPending().intValueExact() == 0);
                    assertTrue(accounts.getCreditsPending().intValueExact() == 1200);
                } else {
                    throw new Exception(String.format("Unexpected account: %s\n",
                            UInt128.asBigInteger(accounts.getId()).toString()));
                }
            }

            // Create an 8th transfer posting the 3rd transfer.
            transfers = new TransferBatch(1);
            transfers.add();
            transfers.setId(8);
            transfers.setPendingId(3);
            transfers.setDebitAccountId(1);
            transfers.setCreditAccountId(2);
            transfers.setLedger(1);
            transfers.setCode(1);
            transfers.setAmount(300);
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

            // Validate account balances after posting 3rd pending transfer.
            accounts = client.lookupAccounts(ids);
            assertTrue(accounts.getLength() == 2);

            while (accounts.next()) {
                if (accounts.getId(UInt128.LeastSignificant) == 1
                        && accounts.getId(UInt128.MostSignificant) == 0) {
                    assertTrue(accounts.getDebitsPosted().intValue() == 400);
                    assertTrue(accounts.getCreditsPosted().intValue() == 0);
                    assertTrue(accounts.getDebitsPending().intValue() == 900);
                    assertTrue(accounts.getCreditsPending().intValue() == 0);
                } else if (accounts.getId(UInt128.LeastSignificant) == 2
                        && accounts.getId(UInt128.MostSignificant) == 0) {
                    assertTrue(accounts.getDebitsPosted().intValue() == 0);
                    assertTrue(accounts.getCreditsPosted().intValue() == 400);
                    assertTrue(accounts.getDebitsPending().intValue() == 0);
                    assertTrue(accounts.getCreditsPending().intValue() == 900);
                } else {
                    throw new Exception(String.format("Unexpected account: %s\n",
                            UInt128.asBigInteger(accounts.getId()).toString()));
                }
            }

            // Create a 9th transfer voiding the 4th transfer.
            transfers = new TransferBatch(1);
            transfers.add();
            transfers.setId(9);
            transfers.setPendingId(4);
            transfers.setDebitAccountId(1);
            transfers.setCreditAccountId(2);
            transfers.setLedger(1);
            transfers.setCode(1);
            transfers.setAmount(400);
            transfers.setFlags(TransferFlags.VOID_PENDING_TRANSFER);

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

            // Validate account balances after voiding 4th pending transfer.
            accounts = client.lookupAccounts(ids);
            assertTrue(accounts.getLength() == 2);

            while (accounts.next()) {
                if (accounts.getId(UInt128.LeastSignificant) == 1
                        && accounts.getId(UInt128.MostSignificant) == 0) {
                    assertTrue(accounts.getDebitsPosted().intValue() == 400);
                    assertTrue(accounts.getCreditsPosted().intValue() == 0);
                    assertTrue(accounts.getDebitsPending().intValue() == 500);
                    assertTrue(accounts.getCreditsPending().intValue() == 0);
                } else if (accounts.getId(UInt128.LeastSignificant) == 2
                        && accounts.getId(UInt128.MostSignificant) == 0) {
                    assertTrue(accounts.getDebitsPosted().intValue() == 0);
                    assertTrue(accounts.getCreditsPosted().intValue() == 400);
                    assertTrue(accounts.getDebitsPending().intValue() == 0);
                    assertTrue(accounts.getCreditsPending().intValue() == 500);
                } else {
                    throw new Exception(String.format("Unexpected account: %s\n",
                            UInt128.asBigInteger(accounts.getId()).toString()));
                }
            }

            // Create a 10th transfer posting the 5th transfer.
            transfers = new TransferBatch(1);
            transfers.add();
            transfers.setId(10);
            transfers.setPendingId(5);
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

            // Validate account balances after posting 5th pending transfer.
            accounts = client.lookupAccounts(ids);
            assertTrue(accounts.getLength() == 2);

            while (accounts.next()) {
                if (accounts.getId(UInt128.LeastSignificant) == 1
                        && accounts.getId(UInt128.MostSignificant) == 0) {
                    assertTrue(accounts.getDebitsPosted().intValueExact() == 900);
                    assertTrue(accounts.getCreditsPosted().intValueExact() == 0);
                    assertTrue(accounts.getDebitsPending().intValueExact() == 0);
                    assertTrue(accounts.getCreditsPending().intValueExact() == 0);
                } else if (accounts.getId(UInt128.LeastSignificant) == 2
                        && accounts.getId(UInt128.MostSignificant) == 0) {
                    assertTrue(accounts.getDebitsPosted().intValueExact() == 0);
                    assertTrue(accounts.getCreditsPosted().intValueExact() == 900);
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
