package com.tigerbeetle;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class TransferFlagsTest {

    @Test
    public void testFlags() {

        assertTrue(TransferFlags.hasLinked(TransferFlags.LINKED));
        assertTrue(TransferFlags.hasLinked(TransferFlags.LINKED | TransferFlags.PENDING));
        assertTrue(TransferFlags
                .hasLinked(TransferFlags.LINKED | TransferFlags.POST_PENDING_TRANSFER));

        assertTrue(TransferFlags.hasPending(TransferFlags.PENDING));
        assertTrue(TransferFlags.hasPending(TransferFlags.PENDING | TransferFlags.LINKED));
        assertTrue(TransferFlags.hasPending(TransferFlags.LINKED | TransferFlags.PENDING));

        assertTrue(TransferFlags.hasPostPendingTransfer(TransferFlags.POST_PENDING_TRANSFER));
        assertTrue(TransferFlags.hasPostPendingTransfer(
                TransferFlags.POST_PENDING_TRANSFER | TransferFlags.LINKED));
        assertTrue(TransferFlags.hasPostPendingTransfer(
                TransferFlags.POST_PENDING_TRANSFER | TransferFlags.PENDING));

        assertTrue(TransferFlags.hasVoidPendingTransfer(TransferFlags.VOID_PENDING_TRANSFER));
        assertTrue(TransferFlags.hasVoidPendingTransfer(
                TransferFlags.VOID_PENDING_TRANSFER | TransferFlags.LINKED));
        assertTrue(TransferFlags.hasVoidPendingTransfer(
                TransferFlags.VOID_PENDING_TRANSFER | TransferFlags.PENDING));

        assertFalse(TransferFlags.hasLinked(TransferFlags.NONE));
        assertFalse(TransferFlags.hasLinked(TransferFlags.POST_PENDING_TRANSFER));
        assertFalse(TransferFlags
                .hasLinked(TransferFlags.PENDING | TransferFlags.POST_PENDING_TRANSFER));

        assertFalse(TransferFlags.hasPending(TransferFlags.NONE));
        assertFalse(TransferFlags.hasPending(TransferFlags.POST_PENDING_TRANSFER));
        assertFalse(TransferFlags
                .hasPending(TransferFlags.LINKED | TransferFlags.POST_PENDING_TRANSFER));

        assertFalse(TransferFlags.hasVoidPendingTransfer(TransferFlags.NONE));
        assertFalse(TransferFlags.hasVoidPendingTransfer(TransferFlags.LINKED));
        assertFalse(
                TransferFlags.hasVoidPendingTransfer(TransferFlags.LINKED | TransferFlags.PENDING));

        assertFalse(TransferFlags.hasPostPendingTransfer(TransferFlags.NONE));
        assertFalse(TransferFlags.hasPostPendingTransfer(TransferFlags.LINKED));
        assertFalse(TransferFlags.hasPostPendingTransfer(
                TransferFlags.LINKED | TransferFlags.VOID_PENDING_TRANSFER));

    }
}
