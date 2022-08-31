package com.tigerbeetle;

import java.util.UUID;

public final class Account {

    private static final UUID ZERO = new UUID(0, 0);

    private UUID id = ZERO;
    private UUID userData = ZERO;
    private int ledger = 0;
    private short code = 0;
    private AccountFlags flags = AccountFlags.None;
    private long creditsPosted = 0;
    private long creditsPending = 0;
    private long debitsPosted = 0;
    private long debitsPending = 0;
    private long timestamp = 0;

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
}
