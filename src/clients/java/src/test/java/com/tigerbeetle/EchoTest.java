package com.tigerbeetle;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class EchoTest {

    static final int HEADER_SIZE = 256; // @sizeOf(vsr.Header)
    static final int TRANSFER_SIZE = 128; // @sizeOf(Transfer)
    static final int MESSAGE_SIZE_MAX = 1024 * 1024; // config.message_size_max
    static final int ITEMS_PER_BATCH = (MESSAGE_SIZE_MAX - HEADER_SIZE) / TRANSFER_SIZE;

    // The number of times the same test is repeated, to stress the
    // cycle of packet exhaustion followed by completions.
    static final int repetitionsMax = 16;

    // The number of concurrency requests on each cycle.
    static final int concurrencyMax = 64;

    @Test(expected = AssertionError.class)
    public void testConstructorNullReplicaAddresses() throws Throwable {

        try (var client = new EchoClient(UInt128.asBytes(0), null, 1)) {

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test(expected = AssertionError.class)
    public void testConstructorInvalidCluster() throws Throwable {
        var clusterInvalid = new byte[] {1, 2, 3};
        try (var client = new EchoClient(clusterInvalid, "3000", 1)) {

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test(expected = AssertionError.class)
    public void testConstructorNegativeConcurrencyMax() throws Throwable {
        try (var client = new EchoClient(UInt128.asBytes(0), "3000", -1)) {

        } catch (Throwable any) {
            throw any;
        }
    }

    @Test
    public void testEchoAccounts() throws Throwable {
        final Random rnd = new Random(1);

        try (var client = new EchoClient(UInt128.asBytes(0), "3000", 32)) {
            final var batch = new AccountBatch(getRandomData(rnd, AccountBatch.Struct.SIZE));
            final var reply = client.echo(batch);
            assertBatchesEqual(batch, reply);
        }
    }

    @Test
    public void testEchoTransfers() throws Throwable {
        final Random rnd = new Random(2);

        try (var client = new EchoClient(UInt128.asBytes(0), "3000", 32)) {
            final var batch = new TransferBatch(getRandomData(rnd, TransferBatch.Struct.SIZE));
            final var future = client.echoAsync(batch);
            final var reply = future.join();
            assertBatchesEqual(batch, reply);
        }
    }

    @Test
    public void testEchoAccountsAsync() throws Throwable {

        final class AsyncContext {
            public AccountBatch batch;
            public CompletableFuture<AccountBatch> future;
        };

        final Random rnd = new Random(3);
        try (var client = new EchoClient(UInt128.asBytes(0), "3000", concurrencyMax)) {
            for (int repetition = 0; repetition < repetitionsMax; repetition++) {

                final var list = new ArrayList<AsyncContext>();
                for (int i = 0; i < concurrencyMax; i++) {

                    // Submitting some random data to be echoed back:
                    final var batch =
                            new AccountBatch(getRandomData(rnd, AccountBatch.Struct.SIZE));

                    var context = new AsyncContext();
                    context.batch = batch;
                    context.future = client.echoAsync(batch);

                    list.add(context);
                }

                for (var context : list) {
                    final var batch = context.batch;
                    final var reply = context.future.get();
                    assertBatchesEqual(batch, reply);
                }
            }
        }
    }

    @Test
    public void testEchoTransfersConcurrent() throws Throwable {

        final class ThreadContext extends Thread {

            public final TransferBatch batch;
            private final EchoClient client;
            private TransferBatch reply;
            private Throwable exception;

            public ThreadContext(EchoClient client, TransferBatch batch) {
                this.client = client;
                this.batch = batch;
                this.reply = null;
                this.exception = null;
            }

            public TransferBatch getReply() {
                if (exception != null)
                    throw new RuntimeException(exception);
                return reply;
            }

            @Override
            public synchronized void run() {
                try {
                    reply = client.echo(batch);
                } catch (Throwable e) {
                    exception = e;
                }
            }
        }

        final Random rnd = new Random(4);
        try (var client = new EchoClient(UInt128.asBytes(0), "3000", concurrencyMax)) {
            for (int repetition = 0; repetition < repetitionsMax; repetition++) {

                final var list = new ArrayList<ThreadContext>();
                for (int i = 0; i < concurrencyMax; i++) {

                    // Submitting some random data to be echoed back:
                    final var batch =
                            new TransferBatch(getRandomData(rnd, TransferBatch.Struct.SIZE));

                    var context = new ThreadContext(client, batch);
                    context.start();

                    list.add(context);
                }

                for (var context : list) {
                    context.join();
                    final var batch = context.batch;
                    final var reply = context.getReply();
                    assertBatchesEqual(batch, reply);
                }
            }
        }
    }

    private ByteBuffer getRandomData(final Random rnd, final int SIZE) {
        final var length = rnd.nextInt(ITEMS_PER_BATCH - 1) + 1;
        var buffer = ByteBuffer.allocateDirect(length * SIZE);
        for (int i = 0; i < length; i++) {
            var item = new byte[SIZE];
            rnd.nextBytes(item);
            buffer.put(item);
        }
        return buffer.position(0);
    }

    private void assertBatchesEqual(Batch batch, Batch reply) {
        final var capacity = batch.getCapacity();
        assertEquals(capacity, reply.getCapacity());

        final var length = batch.getLength();
        assertEquals(length, reply.getLength());

        var buffer = batch.getBuffer();
        var replyBuffer = reply.getBuffer();

        assertEquals(buffer, replyBuffer);
    }
}
