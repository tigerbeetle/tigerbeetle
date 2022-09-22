package com.tigerbeetle;

import org.junit.Assert;
import org.junit.Test;

public class CreateTransferResultTest {

    @Test
    public void testFromValue() {
        var value = 7;
        Assert.assertEquals(CreateTransferResult.DebitAccountIdMustNotBeIntMax,
                CreateTransferResult.fromValue(value));
    }

    @Test
    public void testOrdinal() {
        var value = 15;
        Assert.assertEquals(CreateTransferResult.AmountMustNotBeZero.ordinal(), value);
    }

    @Test(expected = AssertionError.class)
    public void testInvalidValue() {
        var value = 999;
        CreateTransferResult.fromValue(value);
    }


}
