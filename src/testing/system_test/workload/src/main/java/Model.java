import java.math.BigInteger;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Optional;
import java.util.stream.Collectors;

public class Model {
  HashMap<Integer, HashMap<Long, AccountModel>> ledgers = new HashMap<>();

  void accept(Command command) {
    if (command instanceof CreateAccounts) {
      CreateAccounts createAccounts = (CreateAccounts) command;
      for (var newAccount : createAccounts.accounts) {
        var ledger = ledgers.computeIfAbsent(newAccount.ledger, i -> new HashMap<>());

        assert !ledger.containsKey(newAccount.id);

        var account = new AccountModel();
        account.id = newAccount.id;
        account.ledger = newAccount.ledger;
        account.code = newAccount.code;
        account.debitsPosted = BigInteger.ZERO;
        account.creditsPosted = BigInteger.ZERO;
        account.flags = newAccount.flags;

        ledger.put(account.id, account);
      }
    } else if (command instanceof CreateTransfers) {
      CreateTransfers createTransfers = (CreateTransfers) command;
      for (var transfer : createTransfers.transfers) {
        var debitAccount = findAccount(transfer.debitAccountId).orElseThrow();
        var creditAccount = findAccount(transfer.creditAccountId).orElseThrow();
        assert debitAccount.ledger == creditAccount.ledger;

        debitAccount.debitsPosted.add(transfer.amount);
        creditAccount.creditsPosted.add(transfer.amount);
      }
    } else if (command instanceof LookupAccounts) {
      LookupAccounts lookupAccounts = (LookupAccounts) command;
      for (var id : lookupAccounts.ids) {
        assert findAccount(id).isPresent();
      }
    } else {
      throw new IllegalArgumentException("Invalid command: %s".formatted(command.pretty()));
    }
  }


  int accountsCreatedCount() {
    int count = 0;
    for (var ledger : ledgers.values()) {
      count = ledger.size();
    }
    return count;
  }

  AccountModel[] ledgerAccounts(int ledger) {
    var ledgerAccounts = ledgers.get(ledger);
    if (ledgerAccounts != null) {
      var accounts = ledgerAccounts.values().stream()
          .sorted(Comparator.comparing(account -> account.id)).collect(Collectors.toList());
      // Convert from list to typed array.
      var results = new AccountModel[accounts.size()];
      var i = 0;
      for (var account : accounts) {
        results[i] = account;
        i++;
      }
      return results;
    } else {
      return new AccountModel[] {};
    }
  }

  Optional<AccountModel> findAccount(long id) {
    for (var ledger : ledgers.values()) {
      var account = ledger.get(id);
      if (account != null) {
        return Optional.of(account);
      }
    }
    return Optional.empty();
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

