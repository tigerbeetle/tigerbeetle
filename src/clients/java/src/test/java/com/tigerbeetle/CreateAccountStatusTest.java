package com.tigerbeetle;

import org.junit.Assert;
import org.junit.Test;

public class CreateAccountStatusTest {

    @Test
    public void testFromValue() {
        final var value = CreateAccountStatus.Exists.value;
        Assert.assertEquals(CreateAccountStatus.Exists, CreateAccountStatus.fromValue(value));
    }

    @Test
    public void testOrdinal() {
        for (final var expected : CreateAccountStatus.values()) {
            final var actual = CreateAccountStatus.fromValue(expected.value);
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void testMaxValue() {
        var value = 0xFFFFFFFF;
        Assert.assertEquals(CreateAccountStatus.Created, CreateAccountStatus.fromValue(value));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidValue() {
        var value = 999;
        CreateAccountStatus.fromValue(value);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeValue() {
        var value = -100;
        CreateAccountStatus.fromValue(value);
    }
}
