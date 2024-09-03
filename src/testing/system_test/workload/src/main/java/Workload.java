import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import com.tigerbeetle.AccountBatch;
import com.tigerbeetle.AccountFlags;
import com.tigerbeetle.Client;
import com.tigerbeetle.CreateAccountResultBatch;
import com.tigerbeetle.CreateTransferResultBatch;
import com.tigerbeetle.IdBatch;
import com.tigerbeetle.TransferBatch;

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

  void run() {
    for (int i = 0; i < 1_000_000; i++) {
      var command = randomCommand();
      try {
        model.accept(command);
        System.out.printf("Executing: %s\n", command.pretty());
        execute(command);
      } catch (Throwable e) {
        System.out.printf("Failed while executing: %s\n", command.pretty());
        throw e;
      }
    }
  }

  Command randomCommand() {
    // Commands are wrapped in `Optional`, to represent if they are enabled.
    var commandsAll =
        List.of(randomCreateAccounts(), randomCreateTransfers(), randomLookupAccounts());

    // Enabled commands are further wrapped in `Supplier`s, because we don't want to use our entropy
    // to realize all of them, when only one will be selected in the end. They're basically lazy
    // generators.
    //
    // Here we select all commands that are currently enabled.
    var commandsEnabled =
        commandsAll.stream().<Supplier<Command>>mapMulti(Optional::ifPresent).toList();

    // There should always be at least one enabled command.
    assert !commandsEnabled.isEmpty();

    // Select and realize a single command.
    return commandsEnabled.get(random.nextInt(0, commandsEnabled.size())).get();
  }

  Optional<Supplier<Command>> randomCreateAccounts() {
    int accountsCreatedCount = model.accounts.size();

    if (accountsCreatedCount < ACCOUNTS_COUNT_MAX) {
      return Optional.of(() -> {
        var newAccounts = new NewAccount[random.nextInt(1,
            Math.min(ACCOUNTS_COUNT_MAX - accountsCreatedCount + 1, BATCH_SIZE_MAX))];

        for (int i = 0; i < newAccounts.length; i++) {
          var newAccount = new NewAccount();
          newAccount.id = random.nextLong();
          newAccount.ledger = random.nextInt(1, 10);
          newAccount.code = random.nextInt(1, 100);
          newAccount.flags = random.nextBoolean() ? AccountFlags.HISTORY : AccountFlags.NONE;
          newAccounts[i] = newAccount;
        }

        CreateAccounts command = new CreateAccounts();
        command.accounts = newAccounts;
        return command;
      });
    } else {
      return Optional.empty();
    }

  }

  Optional<Supplier<Command>> randomCreateTransfers() {
    // We can only try to transfer within ledgers with at least two accounts.
    var enabledLedgers = model.accountsPerLedger().entrySet().stream()
        .filter(accounts -> accounts.getValue().length > 2).toList();

    if (enabledLedgers.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(() -> {
      var newTransfers = new NewTransfer[random.nextInt(1, BATCH_SIZE_MAX)];

      for (int i = 0; i < newTransfers.length; i++) {
        // For every transfer we pick a random (enabled) ledger.
        var ledger = enabledLedgers.get(random.nextInt(0, enabledLedgers.size()));
        var accounts = ledger.getValue();

        var newTransfer = new NewTransfer();

        newTransfer.id = random.nextLong();
        newTransfer.ledger = ledger.getKey();
        newTransfer.code = random.nextInt(1, 100);
        newTransfer.amount = BigInteger.valueOf(random.nextLong());

        int debitAccountIndex = random.nextInt(0, ledger.getValue().length);
        int creditAccountIndex = random.ints(0, accounts.length)
            .filter((index) -> index != debitAccountIndex).findFirst().orElseThrow();
        newTransfer.debitAccountId = accounts[debitAccountIndex].id;
        newTransfer.creditAccountId = accounts[creditAccountIndex].id;

        newTransfers[i] = newTransfer;
      }

      CreateTransfers command = new CreateTransfers();
      command.transfers = newTransfers;
      return command;
    });
  }

  Optional<Supplier<Command>> randomLookupAccounts() {
    var accounts = model.allAccounts();
    if (accounts.length >= 1) {
      return Optional.of(() -> {
        int lookupBatchSize = random.nextInt(1, Math.min(accounts.length, BATCH_SIZE_MAX) + 1);
        int startIndex =
            accounts.length > lookupBatchSize ? random.nextInt(0, accounts.length - lookupBatchSize)
                : 0;

        var ids = new long[lookupBatchSize];
        for (int i = 0; i < lookupBatchSize; i++) {
          ids[i] = accounts[startIndex + i].id;
        }

        LookupAccounts command = new LookupAccounts();
        command.ids = ids;
        return command;
      });
    }

    return Optional.empty();
  }

  void execute(Command command) {
    if (command instanceof CreateAccounts) {
      CreateAccounts createAccounts = (CreateAccounts) command;

      AccountBatch accounts = new AccountBatch(createAccounts.accounts.length);
      for (NewAccount account : createAccounts.accounts) {
        accounts.add();
        accounts.setId(account.id);
        accounts.setLedger(account.ledger);
        accounts.setCode(account.code);
        accounts.setFlags(account.flags);
      }

      CreateAccountResultBatch accountErrors = client.createAccounts(accounts);
      while (accountErrors.next()) {
        switch (accountErrors.getResult()) {
          default:
            System.err.printf("Error creating account %d: %s\n", accountErrors.getIndex(),
                accountErrors.getResult());
            assert false;
        }
      }
    } else if (command instanceof CreateTransfers) {
      CreateTransfers createTransfers = (CreateTransfers) command;

      TransferBatch transfers = new TransferBatch(createTransfers.transfers.length);
      for (NewTransfer transfer : createTransfers.transfers) {
        transfers.add();
        transfers.setId(transfer.id);
        transfers.setDebitAccountId(transfer.debitAccountId);
        transfers.setCreditAccountId(transfer.creditAccountId);
        transfers.setLedger(transfer.ledger);
        transfers.setCode(transfer.code);
        transfers.setAmount(transfer.amount);
      }

      CreateTransferResultBatch transferErrors = client.createTransfers(transfers);
      while (transferErrors.next()) {
        switch (transferErrors.getResult()) {
          default:
            System.err.printf("Error creating transfer %d: %s\n", transferErrors.getIndex(),
                transferErrors.getResult());
            assert false;
        }
      }

    } else if (command instanceof LookupAccounts) {
      LookupAccounts lookupAccounts = (LookupAccounts) command;

      IdBatch ids = new IdBatch(lookupAccounts.ids.length);
      for (long id : lookupAccounts.ids) {
        ids.add(id);
      }

      AccountBatch accounts = client.lookupAccounts(ids);
      assert accounts.getLength() == lookupAccounts.ids.length;
      // TODO: parse batch and return query results (as model?)
    } else {
      throw new IllegalArgumentException("Invalid command: %s".formatted(command));
    }
  }
}
