import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Tracks information about the accounts created by test, i.e. their ids and which ledgers they
 * belong to.
 */
public class Model {
  /**
   * Accounts by id.
   */
  HashMap<Long, CreatedAccount> accounts = new HashMap<>();

  ArrayList<CreatedAccount> allAccounts() {
    // We use a sorted collection of accounts for stable index-based lookups.
    var allAccounts =
        accounts.values().stream().sorted(Comparator.comparing(account -> account.id())).toList();
    return new ArrayList<CreatedAccount>(allAccounts);
  }

  ArrayList<CreatedAccount> ledgerAccounts(int ledger) {
    // We use a sorted collection of accounts for stable index-based lookups.
    var ledgerAccounts = accounts.values().stream().filter(account -> account.ledger() == ledger)
        .sorted(Comparator.comparing(account -> account.id())).toList();

    return new ArrayList<CreatedAccount>(ledgerAccounts);
  }

  Map<Integer, ArrayList<CreatedAccount>> accountsPerLedger() {
    var ledgerAccounts =
        accounts.values().stream().collect(Collectors.groupingBy(account -> account.ledger()));

    var results = new HashMap<Integer, ArrayList<CreatedAccount>>();

    for (var entry : ledgerAccounts.entrySet()) {
      var values = new ArrayList<CreatedAccount>(entry.getValue());
      results.put(entry.getKey(), values);
    }

    return results;
  }
}


/**
 * Tracks basic information about an account created by our test.
 */
record CreatedAccount(long id, int ledger, int code, int flags) {
}

