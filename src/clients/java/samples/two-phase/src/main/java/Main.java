package com.tigerbeetle.samples;

import java.util.Arrays;
import java.util.UUID;
import java.math.BigInteger;

import com.tigerbeetle.*;

public final class Main {
	public static void main(String[] args)
			throws ConcurrencyExceededException {
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

			CreateAccountResultBatch accountErrors = client.createAccounts(accounts);
			while (accountErrors.next()) {
				switch (accountErrors.getResult()) {
					default:
						System.err.printf("Error creating account %d: %s\n",
								accountErrors.getIndex(),
								accountErrors.getResult());
						assert false;
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

			CreateTransferResultBatch transferErrors = client.createTransfers(transfers);
			while (transferErrors.next()) {
				switch (transferErrors.getResult()) {
					default:
						System.err.printf("Error creating transfer %d: %s\n",
								transferErrors.getIndex(),
								transferErrors.getResult());
						assert false;
				}
			}

			// Validate accounts pending and posted debits/credits before finishing the
			// two-phase transfer
			IdBatch ids = new IdBatch(2);
			ids.add(1);
			ids.add(2);
			accounts = client.lookupAccounts(ids);
			assert accounts.getCapacity() == 2;

			while (accounts.next()) {
				if (accounts.getId(UInt128.LeastSignificant) == 1
						&& accounts.getId(UInt128.MostSignificant) == 0) {
					assert accounts.getDebitsPosted().intValueExact() == 0;
					assert accounts.getCreditsPosted().intValueExact() == 0;
					assert accounts.getDebitsPending().intValueExact() == 500;
					assert accounts.getCreditsPending().intValueExact() == 0;
				} else if (accounts.getId(UInt128.LeastSignificant) == 2
						&& accounts.getId(UInt128.MostSignificant) == 0) {
					assert accounts.getDebitsPosted().intValueExact() == 0;
					assert accounts.getCreditsPosted().intValueExact() == 0;
					assert accounts.getDebitsPending().intValueExact() == 0;
					assert accounts.getCreditsPending().intValueExact() == 500;
				} else {
					System.err.printf("Unexpected account: %s\n",
							UInt128.asBigInteger(accounts.getId()).toString());
					assert false;
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

			transferErrors = client.createTransfers(transfers);
			while (transferErrors.next()) {
				switch (transferErrors.getResult()) {
					default:
						System.err.printf("Error creating transfer %d: %s\n",
								transferErrors.getIndex(),
								transferErrors.getResult());
						assert false;
				}
			}

			// Validate accounts pending and posted debits/credits after finishing the
			// two-phase transfer
			ids = new IdBatch(2);
			ids.add(1);
			ids.add(2);
			accounts = client.lookupAccounts(ids);
			assert accounts.getCapacity() == 2;

			while (accounts.next()) {
				if (accounts.getId(UInt128.LeastSignificant) == 1
						&& accounts.getId(UInt128.MostSignificant) == 0) {
					assert accounts.getDebitsPosted().intValueExact() == 500;
					assert accounts.getCreditsPosted().intValueExact() == 0;
					assert accounts.getDebitsPending().intValueExact() == 0;
					assert accounts.getCreditsPending().intValueExact() == 0;
				} else if (accounts.getId(UInt128.LeastSignificant) == 1
						&& accounts.getId(UInt128.MostSignificant) == 0) {
					assert accounts.getDebitsPosted().intValueExact() == 0;
					assert accounts.getCreditsPosted().intValueExact() == 500;
					assert accounts.getDebitsPending().intValueExact() == 0;
					assert accounts.getCreditsPending().intValueExact() == 0;
				} else {
					System.err.printf("Unexpected account: %s\n",
							UInt128.asBigInteger(accounts.getId()).toString());
					assert false;
				}
			}
		}
	}
}
