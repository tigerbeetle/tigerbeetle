package benchmark;

import java.util.concurrent.CompletableFuture;
import com.tigerbeetle.*;

public class Benchmark {

    public static void main(String[] args) {
        try (var client = new Client(UInt128.asBytes(0), new String[] {"127.0.0.1:3001"})) {

            var accounts = new AccountBatch(2);

            accounts.add();
            accounts.setId(100, 1000);
            accounts.setCode(100);
            accounts.setLedger(720);

            accounts.add();
            accounts.setId(200, 2000);
            accounts.setCode(200);
            accounts.setLedger(720);

            // Async usage:
            // Start the batch ...
            CompletableFuture<CreateAccountResultBatch> future =
                    client.createAccountsAsync(accounts);

            // Register something on the application's side while TigerBeetle is processing
            // UPDATE MyCustomer ...

            var accountErrors = future.get();
            if (accountErrors.getLength() > 0) {

                while (accountErrors.next()) {
                    System.err.printf("Error creating account #%d -> %s\n",
                            accountErrors.getIndex(), accountErrors.getResult());
                }

                return;
            }

            final int HEADER_SIZE = 256; // @sizeOf(vsr.Header)
            final int TRANSFER_SIZE = 128; // @sizeOf(Transfer)
            final int MESSAGE_SIZE_MAX = 1024 * 1024; // config.message_size_max

            final int BATCHES_COUNT = 100;
            final int TRANSFERS_PER_BATCH = (MESSAGE_SIZE_MAX - HEADER_SIZE) / TRANSFER_SIZE;
            final int MAX_TRANSFERS = BATCHES_COUNT * TRANSFERS_PER_BATCH;

            var batches = new TransferBatch[BATCHES_COUNT];
            for (int i = 0; i < BATCHES_COUNT; i++) {
                var transfersBatch = new TransferBatch(TRANSFERS_PER_BATCH);
                for (int j = 0; j < TRANSFERS_PER_BATCH; j++) {

                    transfersBatch.add();
                    transfersBatch.setId(j + 1, i);
                    transfersBatch.setCreditAccountId(100, 1000);
                    transfersBatch.setDebitAccountId(200, 2000);
                    transfersBatch.setCode((short) 1);
                    transfersBatch.setLedger(720);
                    transfersBatch.setAmount(100);
                }

                batches[i] = transfersBatch;
            }

            long totalTime = 0;
            long maxTransferLatency = 0;

            for (var batch : batches) {

                var now = System.currentTimeMillis();

                var transfersErrors = client.createTransfers(batch);
                if (transfersErrors.getLength() > 0) {

                    while (transfersErrors.next()) {
                        System.err.printf("Error creating transfer #%d -> %s%n",
                                transfersErrors.getIndex(), transfersErrors.getResult());
                    }

                    return;
                }

                var elapsed = System.currentTimeMillis() - now;

                totalTime += elapsed;
                if (elapsed > maxTransferLatency)
                    maxTransferLatency = elapsed;
            }

            System.out.println("============================================");

            var result = (long) (MAX_TRANSFERS * 1000) / totalTime;

            System.out.printf("%d transfers per second%n", result);
            System.out.printf("create_transfers max p100 latency per %d transfers = %dms%n",
                    TRANSFERS_PER_BATCH, maxTransferLatency);
            System.out.printf("total %d transfers in %dms%n", MAX_TRANSFERS, totalTime);

        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }
    }

}
