package com.tigerbeetle.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.UUID;

import org.junit.Test;

import com.tigerbeetle.Account;
import com.tigerbeetle.AccountFlags;
import com.tigerbeetle.AccountsBatch;

public class AccountsBatchTest {

    private static Account account1;
    private static Account account2;

    static {
        account1 = new Account();
        account1.setId(UUID.randomUUID());
        account1.setUserData(UUID.randomUUID());
        account1.setLedger(720);
        account1.setCode(1);
        account1.setFlags(AccountFlags.Linked);
        account1.setDebitsPending(100);
        account1.setDebitsPosted(200);
        account1.setCreditsPending(300);
        account1.setCreditsPosted(400);
        account1.setTimestamp(999);

        account2 = new Account();
        account2.setId(UUID.randomUUID());
        account2.setUserData(UUID.randomUUID());
        account2.setLedger(730);
        account2.setCode(2);
        account2.setFlags(AccountFlags.CreditsMustNotExceedDebits);
        account2.setDebitsPending(10);
        account2.setDebitsPosted(20);
        account2.setCreditsPending(30);
        account2.setCreditsPosted(40);
        account2.setTimestamp(99);
    }

    @Test
    public void testAdd() {

        AccountsBatch batch = new AccountsBatch(10);
        assertEquals(batch.getLenght(), 0);
        assertEquals(batch.getCapacity(), 10);

        batch.add(account1);
        assertEquals(batch.getLenght(), 1);

        batch.add(account2);
        assertEquals(batch.getLenght(), 2);

        Account getAccount1 = batch.get(0);
        assertNotNull(getAccount1);

        Account getAccount2 = batch.get(1);
        assertNotNull(getAccount2);

        assertAccounts(account1, getAccount1);
        assertAccounts(account2, getAccount2);
    }

    @Test
    public void testSet() {

        AccountsBatch batch = new AccountsBatch(10);
        assertEquals(batch.getLenght(), 0);
        assertEquals(batch.getCapacity(), 10);

        // Set inndex 0
        batch.set(0, account1);
        assertEquals(batch.getLenght(), 1);

        Account getAccount1 = batch.get(0);
        assertNotNull(getAccount1);

        assertAccounts(account1, getAccount1);

        // Set index 1
        batch.set(1, account1);
        assertEquals(batch.getLenght(), 2);

        // Replace same index 0
        batch.set(0, account2);
        assertEquals(batch.getLenght(), 2);

        Account getAccount2 = batch.get(0);
        assertNotNull(getAccount2);

        assertAccounts(account2, getAccount2);

        // Assert if the index 1 remains unchanged
        Account getAccount3 = batch.get(1);
        assertNotNull(getAccount3);

        assertAccounts(account1, getAccount3);
    }

    @Test
    public void testFromArray() {

        Account[] array = new Account[] { account1, account2 };

        AccountsBatch batch = new AccountsBatch(array);
        assertEquals(batch.getLenght(), 2);
        assertEquals(batch.getCapacity(), 2);

        assertAccounts(account1, batch.get(0));
        assertAccounts(account2, batch.get(1));
    }

    @Test
    public void testToArray() {

        AccountsBatch batch = new AccountsBatch(10);
        assertEquals(batch.getLenght(), 0);
        assertEquals(batch.getCapacity(), 10);

        batch.add(account1);
        batch.add(account2);

        Account[] array = batch.toArray();
        assertAccounts(account1, array[0]);
        assertAccounts(account2, array[1]);
    }

    private static void assertAccounts(Account account1, Account account2) {
        assertEquals(account1.getId(), account2.getId());
        assertEquals(account1.getUserData(), account2.getUserData());
        assertEquals(account1.getLedger(), account2.getLedger());
        assertEquals(account1.getCode(), account2.getCode());
        assertEquals(account1.getFlags(), account2.getFlags());
        assertEquals(account1.getDebitsPending(), account2.getDebitsPending());
        assertEquals(account1.getDebitsPosted(), account2.getDebitsPosted());
        assertEquals(account1.getCreditsPending(), account2.getCreditsPending());
        assertEquals(account1.getCreditsPosted(), account2.getCreditsPosted());
        assertEquals(account1.getTimestamp(), account2.getTimestamp());
    }
}
