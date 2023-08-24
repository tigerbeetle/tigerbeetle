package com.tigerbeetle.samples;

import java.util.Arrays;
import java.math.BigInteger;

import com.tigerbeetle.*;

public final class Main {
    public static void main(String[] args)
		throws RequestException, ConcurrencyExceededException {
	var port = System.getenv("TB_ADDRESS");
	if (port == null || port == "") {
	    port = "3000";
	}

	try (var client = new Client(0, new String[]{port})) {
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

	    // Start five pending transfer.
	    TransferBatch transfers = new TransferBatch(5);
	    transfers.add();
	    transfers.setId(1);
	    transfers.setDebitAccountId(1);
	    transfers.setCreditAccountId(2);
	    transfers.setLedger(1);
	    transfers.setCode(1);
	    transfers.setAmount(500);
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

	    // Validate accounts pending and posted debits/credits
	    // before finishing the two-phase transfer.
	    IdBatch ids = new IdBatch(2);
	    ids.add(1);
	    ids.add(2);
	    accounts = client.lookupAccounts(ids);
	    assert accounts.getCapacity() == 2;

	    while (accounts.next()) {
		if (Arrays.equals(accounts.getId(), UInt128.asBytes(1))) {
		    assert accounts.getDebitsPosted() == 0;
		    assert accounts.getCreditsPosted() == 0;
		    assert accounts.getDebitsPending() == 500;
		    assert accounts.getCreditsPending() == 0;
		} else if (Arrays.equals(accounts.getId(), UInt128.asBytes(2))) {
		    assert accounts.getDebitsPosted() == 0;
		    assert accounts.getCreditsPosted() == 0;
		    assert accounts.getDebitsPending() == 0;
		    assert accounts.getCreditsPending() == 500;
		} else {
		    System.err.printf("Unexpected account: %s\n",
				      UInt128.asBigInteger(accounts.getId()).toString());
		    assert false;
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

	    // Validate account balances after posting 1st pending transfer.
	    accounts = client.lookupAccounts(ids);
	    assert accounts.getCapacity() == 2;

	    while (accounts.next()) {
		if (Arrays.equals(accounts.getId(), UInt128.asBytes(1))) {
		    assert accounts.getDebitsPosted() == 100;
		    assert accounts.getCreditsPosted() == 0;
		    assert accounts.getDebitsPending() == 1400;
		    assert accounts.getCreditsPending() == 0;
		} else if (Arrays.equals(accounts.getId(), UInt128.asBytes(2))) {
		    assert accounts.getDebitsPosted() == 0;
		    assert accounts.getCreditsPosted() == 100;
		    assert accounts.getDebitsPending() == 0;
		    assert accounts.getCreditsPending() == 1400;
		} else {
		    System.err.printf("Unexpected account: %s\n",
				      UInt128.asBigInteger(accounts.getId()).toString());
		    assert false;
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

	    // Validate account balances after voiding 2nd pending transfer.
	    accounts = client.lookupAccounts(ids);
	    assert accounts.getCapacity() == 2;

	    while (accounts.next()) {
		if (Arrays.equals(accounts.getId(), UInt128.asBytes(1))) {
		    assert accounts.getDebitsPosted() == 100;
		    assert accounts.getCreditsPosted() == 0;
		    assert accounts.getDebitsPending() == 1200;
		    assert accounts.getCreditsPending() == 0;
		} else if (Arrays.equals(accounts.getId(), UInt128.asBytes(2))) {
		    assert accounts.getDebitsPosted() == 0;
		    assert accounts.getCreditsPosted() == 100;
		    assert accounts.getDebitsPending() == 0;
		    assert accounts.getCreditsPending() == 1200;
		} else {
		    System.err.printf("Unexpected account: %s\n",
				      UInt128.asBigInteger(accounts.getId()).toString());
		    assert false;
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

	    // Validate account balances after posting 3rd pending transfer.
	    accounts = client.lookupAccounts(ids);
	    assert accounts.getCapacity() == 2;

	    while (accounts.next()) {
		if (Arrays.equals(accounts.getId(), UInt128.asBytes(1))) {
		    assert accounts.getDebitsPosted() == 400;
		    assert accounts.getCreditsPosted() == 0;
		    assert accounts.getDebitsPending() == 900;
		    assert accounts.getCreditsPending() == 0;
		} else if (Arrays.equals(accounts.getId(), UInt128.asBytes(2))) {
		    assert accounts.getDebitsPosted() == 0;
		    assert accounts.getCreditsPosted() == 400;
		    assert accounts.getDebitsPending() == 0;
		    assert accounts.getCreditsPending() == 900;
		} else {
		    System.err.printf("Unexpected account: %s\n",
				      UInt128.asBigInteger(accounts.getId()).toString());
		    assert false;
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

	    // Validate account balances after voiding 4th pending transfer.
	    accounts = client.lookupAccounts(ids);
	    assert accounts.getCapacity() == 2;

	    while (accounts.next()) {
		if (Arrays.equals(accounts.getId(), UInt128.asBytes(1))) {
		    assert accounts.getDebitsPosted() == 400;
		    assert accounts.getCreditsPosted() == 0;
		    assert accounts.getDebitsPending() == 500;
		    assert accounts.getCreditsPending() == 0;
		} else if (Arrays.equals(accounts.getId(), UInt128.asBytes(2))) {
		    assert accounts.getDebitsPosted() == 0;
		    assert accounts.getCreditsPosted() == 400;
		    assert accounts.getDebitsPending() == 0;
		    assert accounts.getCreditsPending() == 500;
		} else {
		    System.err.printf("Unexpected account: %s\n",
				      UInt128.asBigInteger(accounts.getId()).toString());
		    assert false;
		}
	    }

	    // Create a 10th transfer posting the 5th transfer.
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

	    // Validate account balances after posting 5th pending transfer.
	    accounts = client.lookupAccounts(ids);
	    assert accounts.getCapacity() == 2;

	    while (accounts.next()) {
		if (Arrays.equals(accounts.getId(), UInt128.asBytes(1))) {
		    assert accounts.getDebitsPosted() == 900;
		    assert accounts.getCreditsPosted() == 0;
		    assert accounts.getDebitsPending() == 0;
		    assert accounts.getCreditsPending() == 0;
		} else if (Arrays.equals(accounts.getId(), UInt128.asBytes(2))) {
		    assert accounts.getDebitsPosted() == 0;
		    assert accounts.getCreditsPosted() == 900;
		    assert accounts.getDebitsPending() == 0;
		    assert accounts.getCreditsPending() == 0;
		} else {
		    System.err.printf("Unexpected account: %s\n",
				      UInt128.asBigInteger(accounts.getId()).toString());
		    assert false;
		}
	    }
	} catch (Exception e) {
	    assert e == null;
	}
    }
}
