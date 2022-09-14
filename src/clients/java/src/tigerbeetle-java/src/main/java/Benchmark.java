import java.util.UUID;

import com.tigerbeetle.*;

public class Benchmark {

    public static void main(String[] args) {
        try (var client = new Client(0, new String[] {"127.0.0.1:3001"}, 32)) {

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

            final int HEADER_SIZE = 128; // @sizeOf(vsr.Header)
            final int TRANSFER_SIZE = 128; // @sizeOf(Transfer)
            final int MESSAGE_SIZE_MAX = 1024 * 1024; // config.message_size_max

            final int BATCHES_COUNT = 3;
            final int TRANSFERS_PER_BATCH = (MESSAGE_SIZE_MAX - HEADER_SIZE) / TRANSFER_SIZE;
            final int MAX_TRANSFERS = BATCHES_COUNT * TRANSFERS_PER_BATCH;

            var batches = new TransfersBatch[BATCHES_COUNT];
            for (int i = 0; i < BATCHES_COUNT; i++) {
                var batch = new TransfersBatch(TRANSFERS_PER_BATCH);
                for (int j = 0; j < TRANSFERS_PER_BATCH; j++) {
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
                // UPDATE MyCustomer ...

                var errors = request.get();
                if (errors.length > 0)
                    throw new Exception("Unexpected transfer results");
                var elapsed = System.currentTimeMillis() - now;

                totalTime += elapsed;
                if (elapsed > maxTransferLatency)
                    maxTransferLatency = elapsed;
            }

            System.out.println("============================================");

            var result = (long) (MAX_TRANSFERS * 1000) / totalTime;

            System.out.printf("%d transfers per second\n", result);
            System.out.printf("create_transfers max p100 latency per %d transfers = %dms\n",
                    TRANSFERS_PER_BATCH, maxTransferLatency);
            System.out.printf("total %d transfers in %dms\n", MAX_TRANSFERS, totalTime);

        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }
    }

}
