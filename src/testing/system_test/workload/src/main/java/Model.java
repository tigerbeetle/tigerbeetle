import java.math.BigInteger;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Model {
  HashMap<Long, AccountModel> accounts = new HashMap<>();

  void accept(Command command) {
    if (command instanceof CreateAccounts) {
      CreateAccounts createAccounts = (CreateAccounts) command;
      for (var newAccount : createAccounts.accounts) {
        assert !accounts.containsKey(newAccount.id);

        var account = new AccountModel();
        account.id = newAccount.id;
        account.ledger = newAccount.ledger;
        account.code = newAccount.code;
        account.debitsPosted = BigInteger.ZERO;
        account.creditsPosted = BigInteger.ZERO;
        account.flags = newAccount.flags;

        accounts.put(account.id, account);
      }
    } else if (command instanceof CreateTransfers) {
      CreateTransfers createTransfers = (CreateTransfers) command;
      for (var transfer : createTransfers.transfers) {
        var debitAccount = accounts.get(transfer.debitAccountId);
        var creditAccount = accounts.get(transfer.creditAccountId);
        assert debitAccount != null;
        assert creditAccount != null;
        assert debitAccount.ledger == creditAccount.ledger;

        debitAccount.debitsPosted.add(transfer.amount);
        creditAccount.creditsPosted.add(transfer.amount);
      }
    } else if (command instanceof LookupAccounts) {
      LookupAccounts lookupAccounts = (LookupAccounts) command;
      for (var id : lookupAccounts.ids) {
        assert accounts.containsKey(id);
      }
    } else {
      throw new IllegalArgumentException("Invalid command: %s".formatted(command.pretty()));
    }
  }


  AccountModel[] allAccounts() {
    // We use a sorted collection of accounts for deterministic lookups.
    var results = new AccountModel[accounts.size()];
    var i = 0;
    for (var account : accounts.values()) {
      results[i] = account;
      i++;
    }
    return results;
  }

  AccountModel[] ledgerAccounts(int ledger) {
    // We use a sorted collection of accounts (in the given ledger) for deterministic lookups.
    var ledgerAccounts = accounts.values().stream().filter(account -> account.ledger == ledger)
        .sorted(Comparator.comparing(account -> account.id)).toList();

    // Convert from list to typed array.
    var results = new AccountModel[accounts.size()];
    var i = 0;
    for (var account : ledgerAccounts) {
      results[i] = account;
      i++;
    }
    return results;
  }

  Map<Integer, AccountModel[]> accountsPerLedger() {
    var ledgerAccounts =
        accounts.values().stream().collect(Collectors.groupingBy(account -> account.ledger));

    var results = new HashMap<Integer, AccountModel[]>();

    for (var entry : ledgerAccounts.entrySet()) {
      var values = new AccountModel[entry.getValue().size()];
      entry.getValue().toArray(values);
      results.put(entry.getKey(), values);
    }

    return results;
  }
}


class AccountModel {
  long id;
  int ledger;
  int code;
  int flags;
  BigInteger debitsPosted;
  BigInteger creditsPosted;
}

