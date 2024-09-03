import java.math.BigInteger;
import java.util.ArrayList;
import com.tigerbeetle.AccountBatch;
import com.tigerbeetle.Client;
import com.tigerbeetle.CreateAccountResultBatch;
import com.tigerbeetle.CreateTransferResultBatch;
import com.tigerbeetle.IdBatch;
import com.tigerbeetle.TransferBatch;
import com.tigerbeetle.UInt128;

interface Command<Result> {
  Result applyToModel(Model model);

  Result execute(Client client);
}


record CreateAccounts(ArrayList<NewAccount> accounts) implements Command<Void> {

  @Override
  public Void applyToModel(Model model) {
    for (var newAccount : accounts) {
      assert !model.accounts.containsKey(newAccount.id());

      var account = new AccountModel(newAccount.id(), newAccount.ledger(), newAccount.code(),
          newAccount.flags(), BigInteger.ZERO, BigInteger.ZERO);

      model.accounts.put(account.id(), account);
    }
    return null;
  }

  @Override
  public Void execute(Client client) {
    AccountBatch batch = new AccountBatch(accounts.size());
    for (NewAccount account : accounts) {
      batch.add();
      batch.setId(account.id());
      batch.setLedger(account.ledger());
      batch.setCode(account.code());
      batch.setFlags(account.flags());
    }

    CreateAccountResultBatch accountErrors = client.createAccounts(batch);
    while (accountErrors.next()) {
      switch (accountErrors.getResult()) {
        default:
          System.err.printf("Error creating account %d: %s\n", accountErrors.getIndex(),
              accountErrors.getResult());
          assert false;
      }
    }
    return null;
  }
}


record CreateTransfers(ArrayList<NewTransfer> transfers) implements Command<Void> {

  @Override
  public Void applyToModel(Model model) {
    for (var transfer : transfers) {
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

    return null;
  }

  @Override
  public Void execute(Client client) {
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

    CreateTransferResultBatch transferErrors = client.createTransfers(batch);
    while (transferErrors.next()) {
      switch (transferErrors.getResult()) {
        default:
          System.err.printf("Error creating transfer %d: %s\n", transferErrors.getIndex(),
              transferErrors.getResult());
          assert false;
      }
    }

    return null;
  }
}


record LookupAccounts(long[] ids) implements Command<ArrayList<AccountModel>> {

  @Override
  public ArrayList<AccountModel> applyToModel(Model model) {
    assert ids.length > 0;
    var results = new ArrayList<AccountModel>(ids.length);
    for (var id : ids) {
      var account = model.accounts.get(id);
      assert account != null;
      results.add(account);
    }
    return results;
  }

  @Override
  public ArrayList<AccountModel> execute(Client client) {
    IdBatch ids = new IdBatch(this.ids.length);
    for (long id : this.ids) {
      ids.add(id);
    }

    AccountBatch accounts = client.lookupAccounts(ids);
    assert accounts.getLength() == this.ids.length;

    var results = new ArrayList<AccountModel>(accounts.getLength());
    while (accounts.next()) {
      results.add(new AccountModel(accounts.getId(UInt128.LeastSignificant), accounts.getLedger(),
          accounts.getCode(), accounts.getFlags(), accounts.getDebitsPosted(),
          accounts.getCreditsPosted()));
    }
    return results;
  }
}


record NewAccount(long id, int ledger, int code, int flags) {
}


record NewTransfer(long id, long debitAccountId, long creditAccountId, int ledger, int code,
    BigInteger amount, int flags) {
}
