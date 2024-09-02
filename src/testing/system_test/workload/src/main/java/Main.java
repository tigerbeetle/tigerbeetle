import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import com.tigerbeetle.AccountBatch;
import com.tigerbeetle.AccountFlags;
import com.tigerbeetle.Client;
import com.tigerbeetle.CreateAccountResultBatch;
import com.tigerbeetle.CreateTransferResultBatch;
import com.tigerbeetle.IdBatch;
import com.tigerbeetle.TransferBatch;
import com.tigerbeetle.UInt128;

public final class Main {

  static int ACCOUNTS_COUNT_MAX = 1024;
  static int BATCH_SIZE_MAX = 8190;

  public static void main(String[] args) throws Exception {
    Map<String, String> env = System.getenv();
    String replicaAddressesArg = Objects.requireNonNull(env.get("REPLICAS"),
        "REPLICAS environment variable must be set (comma-separated list)");

    String[] replicaAddresses = replicaAddressesArg.split(",");
    if (replicaAddresses.length == 0) {
      throw new IllegalArgumentException(
          "REPLICAS must list at least one address (comma-separated)");
    }

    Random random = new SecureRandom();
    byte[] clusterID = UInt128.asBytes(Long.parseLong(env.getOrDefault("CLUSTER", "1")));

    ArrayList<byte[]> accountsCreatedIds = new ArrayList<byte[]>(ACCOUNTS_COUNT_MAX);

    try (var client = new Client(clusterID, replicaAddresses)) {
      while (true) {
        // Possibly create some accounts
        if (accountsCreatedIds.size() < ACCOUNTS_COUNT_MAX && random.nextBoolean()) {
          int accountBatchSize = random.nextInt(1,
              Math.min(ACCOUNTS_COUNT_MAX - accountsCreatedIds.size() + 1, BATCH_SIZE_MAX));
          AccountBatch accounts = new AccountBatch(accountBatchSize);
          System.out.printf("Creating %d accounts\n", accountBatchSize);
          for (int i = 0; i < accountBatchSize; i++) {
            accounts.add();

            byte[] id = UInt128.asBytes(random.nextLong());
            accounts.setId(id);
            accountsCreatedIds.add(id);

            accounts.setLedger(1);
            accounts.setCode(random.nextInt(1, 100));
            accounts.setFlags(random.nextBoolean() ? AccountFlags.HISTORY : AccountFlags.NONE);

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
        }

        // Possibly create some transfers
        if (accountsCreatedIds.size() >= 2 && random.nextBoolean()) {
          int tranfserBatchSize = random.nextInt(1, BATCH_SIZE_MAX);
          System.out.printf("Creating %d transfers\n", tranfserBatchSize);
          TransferBatch transfers = new TransferBatch(tranfserBatchSize);
          for (int i = 0; i < tranfserBatchSize; i++) {
            transfers.add();

            byte[] id = UInt128.asBytes(random.nextLong());
            transfers.setId(id);

            int debitAccountIndex = random.nextInt(0, accountsCreatedIds.size());
            byte[] debitAccountId = accountsCreatedIds.get(debitAccountIndex);
            byte[] creditAccountId =
                accountsCreatedIds.get(random.ints(0, accountsCreatedIds.size())
                    .filter((index) -> index != debitAccountIndex).findFirst().orElseThrow());

            transfers.setDebitAccountId(debitAccountId);
            transfers.setCreditAccountId(creditAccountId);

            transfers.setLedger(1);
            transfers.setCode(random.nextInt(1, 100));
            transfers.setAmount(random.nextLong());
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
        }

        // Possibly lookup created accounts
        if (accountsCreatedIds.size() >= 1 && random.nextBoolean()) {
          int lookupBatchSize =
              random.nextInt(1, Math.min(accountsCreatedIds.size(), BATCH_SIZE_MAX) + 1);
          int startIndex = accountsCreatedIds.size() > lookupBatchSize
              ? random.nextInt(0, accountsCreatedIds.size() - lookupBatchSize)
              : 0;
          System.out.printf("Looking up accounts %d-%d\n", startIndex,
              startIndex + lookupBatchSize);

          IdBatch ids = new IdBatch(lookupBatchSize);
          for (int i = startIndex; i < startIndex + lookupBatchSize; i++) {
            ids.add(accountsCreatedIds.get(i));
          }
          AccountBatch accounts = client.lookupAccounts(ids);
          assert accounts.getCapacity() == lookupBatchSize;

          var i = 0;
          while (accounts.next()) {
            i++;
            assert accounts.getId() == accountsCreatedIds.get(startIndex + i);
          }
        }
      }
    }
  }
}
