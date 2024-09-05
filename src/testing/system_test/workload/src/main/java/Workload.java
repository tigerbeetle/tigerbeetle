import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;
import com.tigerbeetle.AccountFlags;
import com.tigerbeetle.Client;
import com.tigerbeetle.TransferFlags;

public class Workload {
  static int ACCOUNTS_COUNT_MAX = 100;
  static int BATCH_SIZE_MAX = 100;

  Model model = new Model();
  Random random;
  Client client;

  public Workload(Random random, Client client) {
    this.random = random;
    this.client = client;
  }

  void run() {
    while (true) {
      var command = randomCommand();
      switch (command) {
        case CreateAccounts(var accounts) -> System.out.printf("Creating %d accounts\n",
            accounts.size());
        case CreateTransfers(var transfers) -> System.out.printf("Creating %d transfers\n",
            transfers.size());
        default -> {
          assert false : "unknown command";
        }
      }
      var result = command.execute(client);
      result.reconcile(model);

      lookupAllAccounts().ifPresent(query -> {
        System.out.printf("Querying %d accounts\n", query.ids().length);
        var response = query.execute(client);
        response.reconcile(model);
      });
    }
  }

  Command<?> randomCommand() {
    // Commands are `Supplier`s of values. They are intially wrapped in `Optional`, to represent if
    // they are enabled. Further, they are wrapped in `WithOdds`, increasing the likelyhood of
    // certain commands being chosen.
    var commandsAll = List.of(WithOdds.of(1, createAccounts()), WithOdds.of(5, createTransfers()));

    // Here we select all commands that are currently enabled.
    var commandsEnabled = commandsAll.stream().filter(x -> x.value().isPresent())
        .map(x -> WithOdds.of(x.odds(), x.value().get())).toList();

    // There should always be at least one enabled command.
    assert !commandsEnabled.isEmpty() : "no commands are enabled";

    // Select and realize a single command based on the odds.
    return Arbitrary.odds(random, commandsEnabled).get();
  }

  Optional<Supplier<? extends Command<?>>> createAccounts() {
    int accountsCreatedCount = model.accounts.size();

    if (accountsCreatedCount < ACCOUNTS_COUNT_MAX) {
      return Optional.of(() -> {
        var newAccountsCount = random.nextInt(1,
            Math.min(ACCOUNTS_COUNT_MAX - accountsCreatedCount + 1, BATCH_SIZE_MAX));
        var newAccounts = new ArrayList<NewAccount>(newAccountsCount);

        for (int i = 0; i < newAccountsCount; i++) {
          var id = random.nextLong(0, Long.MAX_VALUE);
          var ledger = random.nextInt(1, 10);
          var code = random.nextInt(1, 100);
          var flags = random.nextInt(0, 10) == 0 ? AccountFlags.HISTORY : AccountFlags.NONE;
          newAccounts.add(new NewAccount(id, ledger, code, flags));
        }

        return new CreateAccounts(newAccounts);
      });
    } else {
      return Optional.empty();
    }

  }

  Optional<Supplier<? extends Command<?>>> createTransfers() {
    // We can only try to transfer within ledgers with at least two accounts.
    var enabledLedgers = model.accountsPerLedger().entrySet().stream()
        .filter(accounts -> accounts.getValue().size() > 2).toList();

    if (enabledLedgers.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(() -> {
      var transfersCount = random.nextInt(1, BATCH_SIZE_MAX);
      var newTransfers = new ArrayList<NewTransfer>(transfersCount);

      for (int i = 0; i < transfersCount; i++) {
        // For every transfer we pick a random (enabled) ledger.
        var ledger = enabledLedgers.get(random.nextInt(0, enabledLedgers.size()));
        var accounts = ledger.getValue();


        var id = random.nextLong(0, Long.MAX_VALUE);
        var ledger2 = ledger.getKey();
        var code = random.nextInt(1, 100);
        var amount = BigInteger.valueOf(random.nextLong(0, Long.MAX_VALUE));
        var flags = random.nextInt(0, 5) == 0 ? TransferFlags.LINKED : TransferFlags.NONE;

        int debitAccountIndex = random.nextInt(0, ledger.getValue().size());
        int creditAccountIndex = random.ints(0, accounts.size())
            .filter((index) -> index != debitAccountIndex).findFirst().orElseThrow();
        var debitAccountId = accounts.get(debitAccountIndex).id();
        var creditAccountId = accounts.get(creditAccountIndex).id();

        newTransfers.add(
            new NewTransfer(id, debitAccountId, creditAccountId, ledger2, code, amount, flags));
      }

      return new CreateTransfers(newTransfers);
    });
  }

  Optional<LookupAccounts> lookupAllAccounts() {
    var accounts = model.allAccounts();
    if (accounts.size() >= 1) {
      var ids = new long[accounts.size()];
      for (int i = 0; i < accounts.size(); i++) {
        ids[i] = accounts.get(i).id();
      }

      return Optional.of(new LookupAccounts(ids));
    }

    return Optional.empty();
  }
}
