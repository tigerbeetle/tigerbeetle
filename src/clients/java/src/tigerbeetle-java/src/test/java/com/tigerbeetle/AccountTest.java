package com.tigerbeetle;

import static org.junit.Assert.assertEquals;
import java.util.UUID;
import org.junit.Test;

public class AccountTest {

    @Test
    public void testDefaultValues() {
        var account = new Account();
        assertEquals(new UUID(0, 0), account.getId());
        assertEquals(new UUID(0, 0), account.getUserData());
        assertEquals(0, account.getLedger());
        assertEquals(AccountFlags.NONE, account.getFlags());
        assertEquals((long) 0, account.getDebitsPosted());
        assertEquals((long) 0, account.getDebitsPending());
        assertEquals((long) 0, account.getCreditsPosted());
        assertEquals((long) 0, account.getCreditsPending());
        assertEquals((long) 0, account.getTimestamp());
    }

    @Test
    public void testId() {
        var account = new Account();
        account.setId(new UUID(100, 200));
        assertEquals(new UUID(100, 200), account.getId());
    }

    @Test(expected = NullPointerException.class)
    public void testIdNull() {
        var account = new Account();
        account.setId(null);
    }

    @Test
    public void testUserData() {
        var account = new Account();
        account.setUserData(new UUID(100, 200));
        assertEquals(new UUID(100, 200), account.getUserData());
    }

    @Test
    public void testUserDataNull() {
        var account = new Account();
        account.setUserData(null);
        assertEquals(new UUID(0, 0), account.getUserData());
    }

    @Test
    public void testLedger() {
        var account = new Account();
        account.setLedger(200);
        assertEquals(200, account.getLedger());
    }

    @Test
    public void testCode() {
        var account = new Account();
        account.setCode(30);
        assertEquals(30, account.getCode());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCodeNegative() {
        var account = new Account();
        account.setCode(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCodeOverflow() {
        var account = new Account();
        account.setCode(Integer.MAX_VALUE);
    }

    @Test
    public void testFlags() {
        var account = new Account();
        account.setFlags(AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS | AccountFlags.LINKED);
        assertEquals((int) (AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS | AccountFlags.LINKED),
                account.getFlags());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFlagsNegative() {
        var account = new Account();
        account.setFlags(-1);
    }

    @Test
    public void testCreditsPending() {
        var account = new Account();
        account.setCreditsPending(999);
        assertEquals((long) 999, account.getCreditsPending());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFlagsOverflow() {
        var account = new Account();
        account.setFlags(Integer.MAX_VALUE);
    }

    @Test
    public void testCreditsPosted() {
        var account = new Account();
        account.setCreditsPosted(999);
        assertEquals((long) 999, account.getCreditsPosted());
    }

    @Test
    public void testDebitsPosted() {
        var account = new Account();
        account.setDebitsPosted(999);
        assertEquals((long) 999, account.getDebitsPosted());
    }

    @Test
    public void testDebitsPending() {
        var account = new Account();
        account.setDebitsPending(999);
        assertEquals((long) 999, account.getDebitsPending());
    }

    @Test
    public void testTimestamp() {
        var account = new Account();
        account.setTimestamp(1234567890);
        assertEquals((long) 1234567890, account.getTimestamp());
    }


}
