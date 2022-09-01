package com.tigerbeetle;

import java.nio.ByteBuffer;
import java.util.UUID;

public final class Transfer {

    static final class Struct {
        public static final int SIZE = 128;
        public static final byte[] RESERVED = new byte[16];
    }

    private static final UUID ZERO = new UUID(0, 0);

    private UUID id;
    private UUID debitAccountId;
    private UUID creditAccountId;
    private UUID userData;
    private UUID pendingId;
    private long timeout;
    private int ledger;
    private short code;
    private short flags;
    private long amount = 0;
    private long timestamp;

    public Transfer() {
        id = ZERO;
        debitAccountId = ZERO;
        creditAccountId = ZERO;
        userData = ZERO;
        pendingId = ZERO;
        timeout = 0;
        ledger = 0;
        code = 0;
        flags = TransferFlags.NONE;
        amount = 0;
        timestamp = 0;
    }

    Transfer(ByteBuffer ptr) {

        id = new UUID(ptr.getLong(), ptr.getLong());
        debitAccountId = new UUID(ptr.getLong(), ptr.getLong());
        creditAccountId = new UUID(ptr.getLong(), ptr.getLong());
        userData = new UUID(ptr.getLong(), ptr.getLong());
        ptr = ptr.position(ptr.position() + Struct.RESERVED.length);
        pendingId = new UUID(ptr.getLong(), ptr.getLong());
        timeout = ptr.getLong();
        ledger = ptr.getInt();
        code = ptr.getShort();
        flags = ptr.getShort();
        amount = ptr.getLong();
        timeout = ptr.getLong();
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        if (id == null)
            throw new NullPointerException();
        this.id = id;
    }

    public UUID getDebitAccountId() {
        return debitAccountId;
    }

    public void setDebitAccountId(UUID debitAccountId) {
        if (debitAccountId == null)
            throw new NullPointerException();
        this.debitAccountId = debitAccountId;
    }

    public UUID getCreditAccountId() {
        return creditAccountId;
    }

    public void setCreditAccountId(UUID creditAccountId) {
        if (creditAccountId == null)
            throw new NullPointerException();
        this.creditAccountId = creditAccountId;
    }

    public UUID getUserData() {
        return userData;
    }

    public void setUserData(UUID userData) {
        if (userData == null) {
            this.userData = ZERO;
        } else {
            this.userData = userData;
        }
    }

    public UUID getPendingId() {
        return pendingId;
    }

    public void setPendingId(UUID pendingId) {
        if (pendingId == null) {
            this.pendingId = ZERO;
        } else {
            this.pendingId = pendingId;
        }
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public int getLedger() {
        return ledger;
    }

    public void setLedger(int ledger) {
        this.ledger = ledger;
    }

    public short getCode() {
        return code;
    }

    public void setCode(short code) {
        this.code = code;
    }

    public short getFlags() {
        return flags;
    }

    public void setFlags(short flags) {
        this.flags = flags;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    void save(ByteBuffer ptr) {
        ptr
                .putLong(id.getMostSignificantBits())
                .putLong(id.getLeastSignificantBits())
                .putLong(debitAccountId.getMostSignificantBits())
                .putLong(debitAccountId.getLeastSignificantBits())
                .putLong(creditAccountId.getMostSignificantBits())
                .putLong(creditAccountId.getLeastSignificantBits())
                .putLong(userData.getMostSignificantBits())
                .putLong(userData.getLeastSignificantBits())
                .put(Struct.RESERVED)
                .putLong(pendingId.getMostSignificantBits())
                .putLong(pendingId.getLeastSignificantBits())
                .putLong(timeout)
                .putInt(ledger)
                .putShort(code)
                .putShort(flags)
                .putLong(amount)
                .putLong(timeout);
    }

}
