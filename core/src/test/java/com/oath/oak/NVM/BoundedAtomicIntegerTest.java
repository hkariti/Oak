package com.oath.oak.NVM;

import org.junit.Test;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

public class BoundedAtomicIntegerTest {
    static BoundedAtomicInteger atomicInt;

    @Test
    public void testConstructor() {
        atomicInt = new BoundedAtomicInteger(100);

        assertTrue(atomicInt instanceof AtomicInteger);
    }

    @Test
    public void testNormalBoundedAdd() {
        atomicInt = new BoundedAtomicInteger(100);

        int prev = atomicInt.getAndAddBounded(10);

        assertEquals(prev, 0);
        assertEquals(atomicInt.get(), 10);
    }

    @Test
    public void testBoundedAddAboveLimit() {
        atomicInt = new BoundedAtomicInteger(100);

        int prev = atomicInt.getAndAddBounded(100);

        assertEquals(prev, 0);
        assertThrows(IllegalArgumentException.class, () -> atomicInt.getAndAddBounded(1));
        assertEquals(atomicInt.get(), 100);
    }
}

