package com.tigerbeetle;

import java.util.UUID;

public final class Account {

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

    public short getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = (short)code;
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
        this.id = id;
    }

    public UUID getUserData() {
        return userData;
    }

    public void setUserData(UUID userData) {
        this.userData = userData;
    }
    
    public int getLedger() {
        return ledger;
    }

    public void setLedger(int ledger) {
        this.ledger = ledger;
    }
}
