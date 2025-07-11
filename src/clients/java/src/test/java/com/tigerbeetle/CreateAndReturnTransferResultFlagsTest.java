package com.tigerbeetle;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class CreateAndReturnTransferResultFlagsTest {

    @Test
    public void testFlags() {
        assertTrue(CreateAndReturnTransferResultFlags
                .hasTransferSet(CreateAndReturnTransferResultFlags.TRANSFER_SET));
        assertTrue(CreateAndReturnTransferResultFlags
                .hasTransferSet(CreateAndReturnTransferResultFlags.TRANSFER_SET
                        | CreateAndReturnTransferResultFlags.ACCOUNT_BALANCES_SET));

        assertTrue(CreateAndReturnTransferResultFlags
                .hasAccountBalancesSet(CreateAndReturnTransferResultFlags.ACCOUNT_BALANCES_SET));
        assertTrue(CreateAndReturnTransferResultFlags
                .hasAccountBalancesSet(CreateAndReturnTransferResultFlags.TRANSFER_SET
                        | CreateAndReturnTransferResultFlags.ACCOUNT_BALANCES_SET));

        assertFalse(CreateAndReturnTransferResultFlags
                .hasTransferSet(CreateAndReturnTransferResultFlags.NONE));
        assertFalse(CreateAndReturnTransferResultFlags
                .hasTransferSet(CreateAndReturnTransferResultFlags.ACCOUNT_BALANCES_SET));

        assertFalse(CreateAndReturnTransferResultFlags
                .hasAccountBalancesSet(CreateAndReturnTransferResultFlags.NONE));
        assertFalse(CreateAndReturnTransferResultFlags
                .hasAccountBalancesSet(CreateAndReturnTransferResultFlags.TRANSFER_SET));
    }
}
