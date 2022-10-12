import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

public class BenchDecode {

    static final int TRANSFER = 128;
    static final int TRANSFER_ID = 16;
    static final int TRANSFER_DEBIT_ID = 16;
    static final int TRANSFER_CREDIT_ID = 16;
    static final int TRANSFER_USER_DATA = 16;
    static final int TRANSFER_RESERVED = 16;
    static final int TRANSFER_PENDING_ID = 16;
    static final int TRANSFER_TIMEOUT = 8;
    static final int TRANSFER_LEDGER = 4;
    static final int TRANSFER_CODE = 2;
    static final int TRANSFER_FLAGS = 2;
    static final int TRANSFER_AMOUNT = 8;
    static final int TRANSFER_TIMESTAMP = 8;

    static final int TRANSFER_ID_OFFSET = 0;
    static final int TRANSFER_DEBIT_ID_OFFSET = 0 + 16;
    static final int TRANSFER_CREDIT_ID_OFFSET = 0 + 16 + 16;
    static final int TRANSFER_USER_DATA_OFFSET = 0 + 16 + 16 + 16;
    static final int TRANSFER_RESERVED_OFFSET = 0 + 16 + 16 + 16 + 16;
    static final int TRANSFER_PENDING_ID_OFFSET = 0 + 16 + 16 + 16 + 16 + 16;
    static final int TRANSFER_TIMEOUT_OFFSET = 0 + 16 + 16 + 16 + 16 + 16 + 16;
    static final int TRANSFER_LEDGER_OFFSET = 0 + 16 + 16 + 16 + 16 + 16 + 16 + 8;
    static final int TRANSFER_CODE_OFFSET = 0 + 16 + 16 + 16 + 16 + 16 + 16 + 8 + 4;
    static final int TRANSFER_FLAGS_OFFSET = 0 + 16 + 16 + 16 + 16 + 16 + 16 + 8 + 4 + 2;
    static final int TRANSFER_AMOUNT_OFFSET = 0 + 16 + 16 + 16 + 16 + 16 + 16 + 8 + 4 + 2 + 2;
    static final int TRANSFER_TIMESTAMP_OFFSET = 0 + 16 + 16 + 16 + 16 + 16 + 16 + 8 + 4 + 2 + 2 + 8;

    public static void main(String[] args) {

        final var buffer = load("transfers");
   
        System.out.println("do this a few times to let JVM optimize...");

        int loops = 10;
        while (loops-- > 0) {
            final var now = System.nanoTime();
            long sum = 0;
            var offset = 0;
            while (offset < buffer.capacity()) {

                final var id_lsb = buffer.getLong(offset + TRANSFER_ID_OFFSET);
                final var id_msb = buffer.getLong(offset + TRANSFER_ID_OFFSET + 8);
                final var credit_id_lsb = buffer.getLong(offset + TRANSFER_CREDIT_ID_OFFSET);
                final var credit_id_msb = buffer.getLong(offset + TRANSFER_CREDIT_ID_OFFSET + 8);
                final var debit_id_lsb = buffer.getLong(offset + TRANSFER_DEBIT_ID_OFFSET);
                final var debit_id_msb = buffer.getLong(offset + TRANSFER_DEBIT_ID_OFFSET + 8);
                final var user_data_lsb = buffer.getLong(offset + TRANSFER_USER_DATA_OFFSET);
                final var user_data_msb = buffer.getLong(offset + TRANSFER_USER_DATA_OFFSET + 8);
                final var reserved_lsb = buffer.getLong(offset + TRANSFER_RESERVED_OFFSET);
                final var reserved_msb = buffer.getLong(offset + TRANSFER_RESERVED_OFFSET + 8);
                final var pending_lsb = buffer.getLong(offset + TRANSFER_PENDING_ID_OFFSET);
                final var pending_msb = buffer.getLong(offset + TRANSFER_PENDING_ID_OFFSET + 8);
                final var timeout = buffer.getLong(offset + TRANSFER_TIMEOUT_OFFSET);
                final var ledger = buffer.getInt(offset + TRANSFER_LEDGER_OFFSET);
                final var code = buffer.getShort(offset + TRANSFER_CODE_OFFSET);
                final var flags = buffer.getShort(offset + TRANSFER_FLAGS_OFFSET);
                final var amount = buffer.getLong(offset + TRANSFER_AMOUNT_OFFSET);
                final var timestamp = buffer.getLong(offset + TRANSFER_TIMESTAMP_OFFSET);

                sum += amount;
                offset += TRANSFER;
            }

            final double elapsed_ms = (System.nanoTime() - now) / (double)TimeUnit.MILLISECONDS.toNanos(1);
            System.out.printf("java: sum of transfer amounts=%s ms=%.3f%n", sum, elapsed_ms);
          
        }        
    }

    private static ByteBuffer load(String file) {
        try (final var stream = new FileInputStream(file)) {
            return ByteBuffer.wrap(stream.readAllBytes()).order(ByteOrder.nativeOrder()).position(0);
        } catch (IOException exception) {
            exception.printStackTrace();
            System.exit(-1);
            return null;
        }
    }
}