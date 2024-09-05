import java.math.BigInteger;
import java.util.ArrayList;
import java.util.BitSet;
import com.tigerbeetle.AccountBatch;
import com.tigerbeetle.Client;
import com.tigerbeetle.CreateAccountResultBatch;
import com.tigerbeetle.IdBatch;
import com.tigerbeetle.TransferBatch;
import com.tigerbeetle.UInt128;

interface Command<CommandResult extends Result> {
  CommandResult execute(Client client);
}


interface Result {
  void reconcile(Model model);
}


record NewAccount(long id, int ledger, int code, int flags) {
}


record CreateAccounts(ArrayList<NewAccount> accounts) implements Command<CreateAccountsResult> {
  @Override
  public CreateAccountsResult execute(Client client) {
    AccountBatch batch = new AccountBatch(accounts.size());
    for (NewAccount account : accounts) {
      batch.add();
      batch.setId(account.id());
      batch.setLedger(account.ledger());
      batch.setCode(account.code());
      batch.setFlags(account.flags());
    }

    CreateAccountResultBatch accountErrors = client.createAccounts(batch);

    // Collect all failure indices.
    var createAccountFailedIndices = new BitSet(accounts.size());
    while (accountErrors.next()) {
      var index = accountErrors.getIndex();
      var result = accountErrors.getResult();
      System.err.printf("create_account with index %d failed: %s\n", index, result);
      createAccountFailedIndices.set(index);
    }

    // We partition the results into created and failed using the bitset.
    var created = new ArrayList<NewAccount>();
    var failed = new ArrayList<NewAccount>();

    int i = 0;
    for (NewAccount account : accounts) {
      if (createAccountFailedIndices.get(i)) {
        failed.add(account);
      } else {
        created.add(account);
      }
      i++;
    }

    return new CreateAccountsResult(created, failed);
  }
}


record CreateAccountsResult(ArrayList<NewAccount> created, ArrayList<NewAccount> failed)
    implements Result {

  @Override
  public void reconcile(Model model) {
    for (var newAccount : created) {
      assert !model.accounts.containsKey(newAccount.id());

      var account = new AccountModel(newAccount.id(), newAccount.ledger(), newAccount.code(),
          newAccount.flags(), BigInteger.ZERO, BigInteger.ZERO);

      model.accounts.put(account.id(), account);
    }
  }

}


record NewTransfer(long id, long debitAccountId, long creditAccountId, int ledger, int code,
    BigInteger amount, int flags) {
}


record CreateTransfers(ArrayList<NewTransfer> transfers) implements Command<CreateTransfersResult> {
  @Override
  public CreateTransfersResult execute(Client client) {
    TransferBatch batch = new TransferBatch(this.transfers.size());
    for (NewTransfer transfer : this.transfers) {
      batch.add();
      batch.setId(transfer.id());
      batch.setDebitAccountId(transfer.debitAccountId());
      batch.setCreditAccountId(transfer.creditAccountId());
      batch.setLedger(transfer.ledger());
      batch.setCode(transfer.code());
      batch.setAmount(transfer.amount());
    }

    var transferErrors = client.createTransfers(batch);

    // Collect all failed transfer indices.
    var transferFailedIndices = new BitSet(transfers.size());
    while (transferErrors.next()) {
      var index = transferErrors.getIndex();
      var result = transferErrors.getResult();
      System.err.printf("create_transfer with index %d failed: %s\n", index, result);
      transferFailedIndices.set(index);
    }

    // We partition the results into created and failed using the bitset.
    var created = new ArrayList<NewTransfer>();
    var failed = new ArrayList<NewTransfer>();

    int i = 0;
    for (NewTransfer transfer : this.transfers) {
      if (transferFailedIndices.get(i)) {
        failed.add(transfer);
      } else {
        created.add(transfer);
      }
      i++;
    }

    return new CreateTransfersResult(created, failed);
  }
}


record CreateTransfersResult(ArrayList<NewTransfer> created, ArrayList<NewTransfer> failed)
    implements Result {

  @Override
  public void reconcile(Model model) {
    for (var transfer : created) {
      var debitAccount = model.accounts.get(transfer.debitAccountId());
      var creditAccount = model.accounts.get(transfer.creditAccountId());
      assert debitAccount != null;
      assert creditAccount != null;
      assert debitAccount.ledger() == creditAccount.ledger();

      // Update debitsPosted
      model.accounts.put(debitAccount.id(),
          new AccountModel(debitAccount.id(), debitAccount.ledger(), debitAccount.code(),
              debitAccount.flags(), debitAccount.debitsPosted().add(transfer.amount()),
              debitAccount.creditsPosted()));

      // Update creditsPosted
      model.accounts.put(creditAccount.id(),
          new AccountModel(creditAccount.id(), creditAccount.ledger(), creditAccount.code(),
              creditAccount.flags(), creditAccount.debitsPosted(),
              creditAccount.creditsPosted().add(transfer.amount())));
    }
  }
}


record LookupAccounts(long[] ids) implements Command<LookupAccountsResult> {

  @Override
  public LookupAccountsResult execute(Client client) {
    IdBatch ids = new IdBatch(this.ids.length);
    for (long id : this.ids) {
      ids.add(id);
    }

    AccountBatch accounts = client.lookupAccounts(ids);

    // We assume all ids we lookup are from successfully created accounts.
    assert accounts.getLength() == this.ids.length : "expected all ids of lookup_accounts to exist";

    var results = new ArrayList<AccountModel>(accounts.getLength());
    while (accounts.next()) {
      results.add(new AccountModel(accounts.getId(UInt128.LeastSignificant), accounts.getLedger(),
          accounts.getCode(), accounts.getFlags(), accounts.getDebitsPosted(),
          accounts.getCreditsPosted()));
    }

    return new LookupAccountsResult(results);
  }
}


record LookupAccountsResult(ArrayList<AccountModel> accountsFound) implements Result {
  @Override
  public void reconcile(Model model) {
    for (var account : accountsFound) {
      assert model.accounts.containsKey(account.id());
    }
  }

}

