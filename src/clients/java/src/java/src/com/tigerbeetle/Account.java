package com.tigerbeetle;

import java.nio.ByteBuffer;
import java.util.UUID;

public final class Account {

    static final class Struct {
        public static final int SIZE = 128;
        public static final byte[] RESERVED = new byte[48];
    }

    private static final UUID ZERO = new UUID(0, 0);

    private UUID id;
    private UUID userData;
    private int ledger;
    private short code;
    private AccountFlags flags;
    private long creditsPosted;
    private long creditsPending;
    private long debitsPosted;
    private long debitsPending;
    private long timestamp;

    public Account() {
        id = ZERO;
        userData = ZERO;
        ledger = 0;
        code = 0;
        flags = AccountFlags.None;
        creditsPosted = 0;
        creditsPending = 0;
        debitsPosted = 0;
        debitsPending = 0;
        timestamp = 0;
    }

    Account(ByteBuffer ptr) {
        id = new UUID(ptr.getLong(), ptr.getLong());
        userData = new UUID(ptr.getLong(), ptr.getLong());
        ptr = ptr.position(ptr.position() + Struct.RESERVED.length);
        ledger = ptr.getInt();
        code = ptr.getShort();
        flags = AccountFlags.fromValue(ptr.getShort());
        debitsPending = ptr.getLong();
        debitsPosted = ptr.getLong();
        creditsPending = ptr.getLong();
        creditsPosted = ptr.getLong();
        timestamp = ptr.getLong();
    }

    public short getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = (short) code;
    }

    public AccountFlags getFlags() {
        return flags;
    }

    public void setFlags(AccountFlags flags) {
        this.flags = flags;
    }

    public long getDebitsPending() {
        return debitsPending;
    }

    public void setDebitsPending(long debitsPending) {
        this.debitsPending = debitsPending;
    }

    public long getDebitsPosted() {
        return debitsPosted;
    }

    public void setDebitsPosted(long debitsPosted) {
        this.debitsPosted = debitsPosted;
    }

    public long getCreditsPending() {
        return creditsPending;
    }

    public void setCreditsPending(long creditsPending) {
        this.creditsPending = creditsPending;
    }

    public long getCreditsPosted() {
        return creditsPosted;
    }

    public void setCreditsPosted(long creditsPosted) {
        this.creditsPosted = creditsPosted;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        if (id == null)
            throw new NullPointerException();
        this.id = id;
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

    public int getLedger() {
        return ledger;
    }

    public void setLedger(int ledger) {
        this.ledger = ledger;
    }

    void save(ByteBuffer ptr) {

        ptr
                .putLong(id.getMostSignificantBits())
                .putLong(id.getLeastSignificantBits())
                .putLong(userData.getMostSignificantBits())
                .putLong(userData.getLeastSignificantBits())
                .put(Struct.RESERVED)
                .putInt(ledger)
                .putShort(code)
                .putShort(flags.value)
                .putLong(debitsPending)
                .putLong(debitsPosted)
                .putLong(creditsPending)
                .putLong(creditsPosted)
                .putLong(timestamp);
    }
}
