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
  static int ACCOUNTS_COUNT_MAX = 1024;
  static int BATCH_SIZE_MAX = 8190;

  Model model = new Model();
  Random random;
  Client client;

  public Workload(Random random, Client client) {
    this.random = random;
    this.client = client;
  }

  @SuppressWarnings("unchecked")
  void run() {
    for (int iteration = 0; true; iteration++) {
      var command = randomCommand();
      try {
        var modelResult = command.applyToModel(model);
        var systemResult = command.execute(client);

        if (iteration > 0 && iteration % 100 == 0) {
          System.out.printf("At %d iterations...\n", iteration);
        }

        if (command instanceof LookupAccounts) {
          assert modelResult.getClass() == systemResult.getClass();
          var modelAccounts = (ArrayList<AccountModel>) modelResult;
          var systemAccounts = (ArrayList<AccountModel>) systemResult;
          assert modelAccounts.size() == systemAccounts.size();

          for (int i = 0; i < modelAccounts.size(); i++) {
            var modelAccount = modelAccounts.get(i);
            var systemAccount = systemAccounts.get(i);
            assert modelAccount.equals(systemAccount) : "\n model %s\n!=\nsystem %s"
                .formatted(modelAccount, systemAccount);
          }
        }
      } catch (Throwable e) {
        System.err.printf("Failed while executing: %s\n", command);
        throw e;
      }
    }
  }

  Command<?> randomCommand() {
    // Commands are wrapped in `Optional`, to represent if they are enabled.
    var commandsAll =
        List.of(randomCreateAccounts(), randomCreateTransfers(), randomLookupAccounts());

    // Enabled commands are further wrapped in `Supplier`s, because we don't want to use our entropy
    // to realize all of them, when only one will be selected in the end. They're basically lazy
    // generators.
    //
    // Here we select all commands that are currently enabled.
    var commandsEnabled =
        commandsAll.stream().<Supplier<? extends Command<?>>>mapMulti(Optional::ifPresent).toList();

    // There should always be at least one enabled command.
    assert !commandsEnabled.isEmpty() : "no commands are enabled";

    // Select and realize a single command.
    return commandsEnabled.get(random.nextInt(0, commandsEnabled.size())).get();
  }

  Optional<Supplier<? extends Command<?>>> randomCreateAccounts() {
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
          var flags = random.nextBoolean() ? AccountFlags.HISTORY : AccountFlags.NONE;
          newAccounts.add(new NewAccount(id, ledger, code, flags));
        }

        return new CreateAccounts(newAccounts);
      });
    } else {
      return Optional.empty();
    }

  }

  Optional<Supplier<? extends Command<?>>> randomCreateTransfers() {
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
        var flags = random.nextBoolean() ? TransferFlags.LINKED : TransferFlags.NONE;

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

  Optional<Supplier<? extends Command<?>>> randomLookupAccounts() {
    var accounts = model.allAccounts();
    if (accounts.size() >= 1) {
      return Optional.of(() -> {
        int lookupBatchSize = random.nextInt(1, Math.min(accounts.size(), BATCH_SIZE_MAX) + 1);
        int startIndex =
            accounts.size() > lookupBatchSize ? random.nextInt(0, accounts.size() - lookupBatchSize)
                : 0;

        var ids = new long[lookupBatchSize];
        for (int i = 0; i < lookupBatchSize; i++) {
          ids[i] = accounts.get(startIndex + i).id();
        }

        return new LookupAccounts(ids);
      });
    }

    return Optional.empty();
  }
}
