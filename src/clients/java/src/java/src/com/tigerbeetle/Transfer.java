package com.tigerbeetle;

import java.util.UUID;

public final class Transfer {

    private static final UUID ZERO = new UUID(0, 0);

    private UUID id = ZERO;
    private UUID debitAccountId = ZERO;
    private UUID creditAccountId = ZERO;
    private UUID userData = ZERO;
    private UUID pendingId = ZERO;
    private long timeout = 0;
    private int ledger = 0;
    private short code = 0;
    private short flags = TransferFlags.NONE;
    private long amount = 0;
    private long timestamp = 0;

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

}
