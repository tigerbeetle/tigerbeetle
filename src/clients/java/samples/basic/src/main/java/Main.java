package com.tigerbeetle.samples;

import java.util.Arrays;
import java.math.BigInteger;

import com.tigerbeetle.*;

public final class Main {
    public static void main(String[] args) throws com.tigerbeetle.RequestException {
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

	    TransferBatch transfers = new TransferBatch(1);
	    transfers.add();
	    transfers.setId(1);
	    transfers.setDebitAccountId(1);
	    transfers.setCreditAccountId(2);
	    transfers.setLedger(1);
	    transfers.setCode(1);
	    transfers.setAmount(10);

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

	    IdBatch ids = new IdBatch(2);
	    ids.add(1);
	    ids.add(2);
	    accounts = client.lookupAccounts(ids);
	    assert accounts.getCapacity() == 2;

	    while (accounts.next()) {
		if (Arrays.equals(accounts.getId(), UInt128.asBytes(1))) {
		    assert accounts.getDebitsPosted() == 10;
		    assert accounts.getCreditsPosted() == 0;
		} else if (Arrays.equals(accounts.getId(), UInt128.asBytes(2))) {
		    assert accounts.getDebitsPosted() == 0;
		    assert accounts.getCreditsPosted() == 10;
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
