package com.tigerbeetle;

import org.junit.Assert;
import org.junit.Test;

public class CreateTransferStatusTest {

    @Test
    public void testFromValue() {
        var value = CreateTransferStatus.DebitAccountIdMustNotBeIntMax.value;
        Assert.assertEquals(CreateTransferStatus.DebitAccountIdMustNotBeIntMax,
                CreateTransferStatus.fromValue(value));
    }

    @Test
    public void testOrdinal() {
        for (final var expected : CreateTransferStatus.values()) {
            final var actual = CreateTransferStatus.fromValue(expected.value);
            Assert.assertEquals(expected, actual);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidValue() {
        var value = 999;
        CreateTransferStatus.fromValue(value);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeValue() {
        var value = -1;
        CreateTransferStatus.fromValue(value);
    }
}
