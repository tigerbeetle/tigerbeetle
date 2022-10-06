package com.tigerbeetle;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class AccountFlagsTest {

    @Test
    public void testFlags() {

        assertTrue(AccountFlags.hasLinked(AccountFlags.LINKED));
        assertTrue(AccountFlags
                .hasLinked(AccountFlags.LINKED | AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS));
        assertTrue(AccountFlags
                .hasLinked(AccountFlags.LINKED | AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS));

        assertTrue(AccountFlags
                .hasDebitsMustNotExceedCredits(AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS));
        assertTrue(AccountFlags.hasDebitsMustNotExceedCredits(
                AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS | AccountFlags.LINKED));
        assertTrue(AccountFlags
                .hasDebitsMustNotExceedCredits(AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS
                        | AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS));

        assertTrue(AccountFlags
                .hasCreditsMustNotExceedDebits(AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS));
        assertTrue(AccountFlags.hasCreditsMustNotExceedDebits(
                AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS | AccountFlags.LINKED));
        assertTrue(AccountFlags
                .hasCreditsMustNotExceedDebits(AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS
                        | AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS));

        assertFalse(AccountFlags.hasLinked(AccountFlags.NONE));
        assertFalse(AccountFlags.hasLinked(AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS));
        assertFalse(AccountFlags.hasLinked(AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS
                | AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS));

        assertFalse(AccountFlags.hasDebitsMustNotExceedCredits(AccountFlags.NONE));
        assertFalse(AccountFlags.hasDebitsMustNotExceedCredits(AccountFlags.LINKED));
        assertFalse(AccountFlags.hasDebitsMustNotExceedCredits(
                AccountFlags.LINKED | AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS));

        assertFalse(AccountFlags.hasCreditsMustNotExceedDebits(AccountFlags.NONE));
        assertFalse(AccountFlags.hasCreditsMustNotExceedDebits(AccountFlags.LINKED));
        assertFalse(AccountFlags.hasCreditsMustNotExceedDebits(
                AccountFlags.LINKED | AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS));

    }

}
