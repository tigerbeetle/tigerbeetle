package com.tigerbeetle.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.UUID;

import org.junit.Test;

import com.tigerbeetle.Account;
import com.tigerbeetle.AccountFlags;
import com.tigerbeetle.AccountsBatch;
import com.tigerbeetle.Transfer;
import com.tigerbeetle.TransferFlags;
import com.tigerbeetle.TransfersBatch;

public class TransfersBatchTest {

    private static Transfer transfer1;
    private static Transfer transfer2;

    static {
        transfer1 = new Transfer();
        transfer1.setId(UUID.randomUUID());
        transfer1.setCreditAccountId(UUID.randomUUID());
        transfer1.setDebitAccountId(UUID.randomUUID());
        transfer1.setUserData(UUID.randomUUID());
        transfer1.setAmount(1000);
        transfer1.setCode((short)10);
        transfer1.setLedger(720);

        transfer2 = new Transfer();
        transfer2.setId(UUID.randomUUID());
        transfer2.setCreditAccountId(UUID.randomUUID());
        transfer2.setDebitAccountId(UUID.randomUUID());
        transfer2.setUserData(UUID.randomUUID());
        transfer2.setAmount(200);
        transfer2.setCode((short)20);
        transfer2.setLedger(100);
        transfer2.setFlags(new TransferFlags().setPending(true).setLinked(true).getValue());
        transfer2.setPendingId(transfer1.getId());
        transfer2.setTimeout(2500);
        transfer2.setTimestamp(900);
    }

    @Test
    public void testAdd() {

        TransfersBatch batch = new TransfersBatch(10);
        assertEquals(batch.getLenght(), 0);
        assertEquals(batch.getCapacity(), 10);

        batch.add(transfer1);
        assertEquals(batch.getLenght(), 1);

        batch.add(transfer2);
        assertEquals(batch.getLenght(), 2);

        Transfer getTransfer1 = batch.get(0);
        assertNotNull(getTransfer1);

        Transfer getTransfer2 = batch.get(1);
        assertNotNull(getTransfer2);

        assertTransfers(transfer1, getTransfer1);
        assertTransfers(transfer2, getTransfer2);
    }

    @Test
    public void testSet() {

        TransfersBatch batch = new TransfersBatch(10);
        assertEquals(batch.getLenght(), 0);
        assertEquals(batch.getCapacity(), 10);

        // Set inndex 0
        batch.set(0, transfer1);
        assertEquals(batch.getLenght(), 1);

        Transfer getTransfer1 = batch.get(0);
        assertNotNull(getTransfer1);

        assertTransfers(transfer1, getTransfer1);

        // Set index 1
        batch.set(1, transfer1);
        assertEquals(batch.getLenght(), 2);

        // Replace same index 0
        batch.set(0, transfer2);
        assertEquals(batch.getLenght(), 2);

        Transfer getTransfer2 = batch.get(0);
        assertNotNull(getTransfer2);

        assertTransfers(transfer2, getTransfer2);

        // Assert if the index 1 remains unchanged
        Transfer getTransfer3 = batch.get(1);
        assertNotNull(getTransfer3);

        assertTransfers(transfer1, getTransfer3);
    }

    @Test
    public void testFromArray() {

        Transfer[] array = new Transfer[] { transfer1, transfer2 };

        TransfersBatch batch = new TransfersBatch(array);
        assertEquals(batch.getLenght(), 2);
        assertEquals(batch.getCapacity(), 2);

        assertTransfers(transfer1, batch.get(0));
        assertTransfers(transfer2, batch.get(1));
    }

    @Test
    public void testToArray() {

        TransfersBatch batch = new TransfersBatch(10);
        assertEquals(batch.getLenght(), 0);
        assertEquals(batch.getCapacity(), 10);

        batch.add(transfer1);
        batch.add(transfer2);

        Transfer[] array = batch.toArray();
        assertTransfers(transfer1, array[0]);
        assertTransfers(transfer2, array[1]);
    }

    private static void assertTransfers(Transfer transfer1, Transfer transfer2) {
        assertEquals(transfer1.getId(), transfer2.getId());
        assertEquals(transfer1.getCreditAccountId(), transfer2.getCreditAccountId());
        assertEquals(transfer1.getDebitAccountId(), transfer2.getDebitAccountId());        
        assertEquals(transfer1.getUserData(), transfer2.getUserData());
        assertEquals(transfer1.getLedger(), transfer2.getLedger());
        assertEquals(transfer1.getCode(), transfer2.getCode());
        assertEquals(transfer1.getFlags(), transfer2.getFlags());
        assertEquals(transfer1.getAmount(), transfer2.getAmount());
        assertEquals(transfer1.getTimeout(), transfer2.getTimeout());
        assertEquals(transfer1.getPendingId(), transfer2.getPendingId());
        assertEquals(transfer1.getTimestamp(), transfer2.getTimestamp());
    }
}
