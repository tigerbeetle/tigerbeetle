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

    @Test
    public void testMaxValue() {
        var value = 0xFFFFFFFF;
        Assert.assertEquals(CreateTransferStatus.Created, CreateTransferStatus.fromValue(value));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidValue() {
        var value = 999;
        CreateTransferStatus.fromValue(value);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeValue() {
        var value = -100;
        CreateTransferStatus.fromValue(value);
    }
}
