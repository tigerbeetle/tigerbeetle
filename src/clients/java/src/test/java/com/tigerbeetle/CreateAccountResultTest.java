package com.tigerbeetle;

import org.junit.Assert;
import org.junit.Test;

public class CreateAccountResultTest {

    @Test
    public void testFromValue() {
        final var value = CreateAccountResult.Exists.value;
        Assert.assertEquals(CreateAccountResult.Exists, CreateAccountResult.fromValue(value));
    }

    @Test
    public void testOrdinal() {
        final var value = CreateAccountResult.Exists.value;
        Assert.assertEquals(CreateAccountResult.Exists.ordinal(), value);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidValue() {
        var value = 999;
        CreateAccountResult.fromValue(value);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeValue() {
        var value = -1;
        CreateAccountResult.fromValue(value);
    }
}
