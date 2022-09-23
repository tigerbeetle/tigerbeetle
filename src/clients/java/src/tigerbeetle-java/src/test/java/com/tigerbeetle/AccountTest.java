package com.tigerbeetle;

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import com.tigerbeetle.UInt128.Bytes;

public class AccountTest {

    @Test
    public void testDefaultValues() {
        var account = new Account();
        assertEquals(0L, account.getId(Bytes.LeastSignificant));
        assertEquals(0L, account.getId(Bytes.MostSignificant));
        assertEquals(0L, account.getUserData(Bytes.LeastSignificant));
        assertEquals(0L, account.getUserData(Bytes.MostSignificant));
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
        account.setId(100, 200);
        assertEquals(100L, account.getId(Bytes.LeastSignificant));
        assertEquals(200L, account.getId(Bytes.MostSignificant));
    }

    @Test(expected = NullPointerException.class)
    public void testIdNull() {
        byte[] uuid = null;
        var account = new Account();
        account.setId(uuid);
    }

    @Test
    public void testUserData() {
        var account = new Account();
        account.setUserData(100, 200);
        assertEquals(100L, account.getUserData(Bytes.LeastSignificant));
        assertEquals(200L, account.getUserData(Bytes.MostSignificant));
    }

    @Test
    public void testUserDataNull() {
        byte[] userData = null;
        var account = new Account();
        account.setUserData(userData);
        assertEquals(0L, account.getUserData(Bytes.LeastSignificant));
        assertEquals(0L, account.getUserData(Bytes.MostSignificant));
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
