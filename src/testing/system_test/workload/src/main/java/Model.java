import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Model {
  HashMap<Long, AccountModel> accounts = new HashMap<>();

  ArrayList<AccountModel> allAccounts() {
    // We use a sorted collection of accounts for stable index-based lookups.
    var allAccounts =
        accounts.values().stream().sorted(Comparator.comparing(account -> account.id())).toList();
    return new ArrayList<AccountModel>(allAccounts);
  }

  ArrayList<AccountModel> ledgerAccounts(int ledger) {
    // We use a sorted collection of accounts for stable index-based lookups.
    var ledgerAccounts = accounts.values().stream().filter(account -> account.ledger() == ledger)
        .sorted(Comparator.comparing(account -> account.id())).toList();

    return new ArrayList<AccountModel>(ledgerAccounts);
  }

  Map<Integer, ArrayList<AccountModel>> accountsPerLedger() {
    var ledgerAccounts =
        accounts.values().stream().collect(Collectors.groupingBy(account -> account.ledger()));

    var results = new HashMap<Integer, ArrayList<AccountModel>>();

    for (var entry : ledgerAccounts.entrySet()) {
      var values = new ArrayList<AccountModel>(entry.getValue());
      results.put(entry.getKey(), values);
    }

    return results;
  }
}


record AccountModel(long id, int ledger, int code, int flags, BigInteger debitsPosted,
    BigInteger creditsPosted) {
}

