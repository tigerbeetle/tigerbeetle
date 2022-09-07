import java.util.UUID;

import com.tigerbeetle.*;

public class Benchmark {

    public static void main(String[] args) {
        try (var client = new Client(0, new String[] { "127.0.0.1:3001" }, 32)) {

            var accounts = new AccountsBatch(2);

            var account1 = new Account();
            account1.setId(UUID.randomUUID());
            account1.setCode(100);
            account1.setLedger(720);
            accounts.add(account1);

            var account2 = new Account();
            account2.setId(UUID.randomUUID());
            account2.setCode(200);
            account2.setLedger(720);
            accounts.add(account2);

            var results = client.createAccounts(accounts);
            if (results.length > 0)
                throw new Exception("Unexpected createAccount results");

            final int max_batches = 3;
            final int max_transfers_per_batch = 8191; // config.message_size_max - @sizeOf(vsr.Header)
            var batches = new TransfersBatch[max_batches];
            for (int i = 0; i < max_batches; i++) {
                var batch = new TransfersBatch(max_transfers_per_batch);
                for (int j = 0; j < max_transfers_per_batch; j++) {
                    var transfer = new Transfer();
                    transfer.setId(new UUID(i + 1, j + 1));
                    transfer.setCreditAccountId(account1.getId());
                    transfer.setDebitAccountId(account2.getId());
                    transfer.setCode((short) 1);
                    transfer.setLedger(720);
                    transfer.setAmount(100);
                    batch.add(transfer);
                }

                batches[i] = batch;
            }

            long totalTime = 0;
            long maxTransferLatency = 0;

            for (var batch : batches) {

                var now = System.currentTimeMillis();

                // Async usage:
                // Start the batch ...
                var request = client.createTransfersAsync(batch);

                // Register something on the application's side while tigerbeetle is processing
                // UPDATE FROM MyCustomer ...

                var errors = request.get();
                if (errors.length > 0)
                    throw new Exception("Unexpected transfer results");
                var elapsed = System.currentTimeMillis() - now;

                totalTime += elapsed;
                if (elapsed > maxTransferLatency)
                    maxTransferLatency = elapsed;
            }

            System.out.println("============================================");

            var result = (long) (max_batches * max_transfers_per_batch * 1000) / totalTime;

            System.out.printf("%d transfers per second\n", result);
            System.out.printf("create_transfers max p100 latency per %d transfers = %dms\n", max_transfers_per_batch,
                    maxTransferLatency);
            System.out.printf("total %d transfers in %dms\n", max_batches * max_transfers_per_batch, totalTime);

        } catch (InitializationException | RequestException | Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }
    }

}
