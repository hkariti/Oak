package com.oath.oak.NVM;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.lang.IllegalArgumentException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

public class EntryKeyListTest {
    static EntryKeyList entryList;

    @Test
    public void testBufferConstructor() {
        ByteBuffer buf = ByteBuffer.allocate(8 * 10);

        entryList = new EntryKeyList(buf);

        assertEquals(entryList.entries(), 10);
    }

    @Test
    public void testEnableEntry() {
        ByteBuffer buf = ByteBuffer.allocate(8 * 10);

        entryList = new EntryKeyList(buf);
        entryList.enable(4);

        assertEquals(entryList.isActive(4), true);
    }

    @Test
    public void testDisableEntry() {
        ByteBuffer buf = ByteBuffer.allocate(8 * 10);

        entryList = new EntryKeyList(buf);
        entryList.enable(4);
        entryList.disable(4);

        assertEquals(entryList.isActive(4), false);
    }

    @Test
    public void testAccessOutOfBounds() {
        ByteBuffer buf = ByteBuffer.allocate(8 * 10);

        entryList = new EntryKeyList(buf);
        assertThrows(IllegalArgumentException.class, () -> {
            entryList.enable(40);
        });
    }

    @Test
    public void testWritePointer() {
        ByteBuffer buf = ByteBuffer.allocate(8 * 10);
        byte pointer;

        entryList = new EntryKeyList(buf);
        entryList.pointer(4, (byte)123);
        pointer = entryList.pointer(4);

        assertEquals(pointer, 123);
        assertNotEquals(entryList.pointer(3), 123);
    }

    @Test
    public void testWriteBadPointer() {
        ByteBuffer buf = ByteBuffer.allocate(8 * 10);
        byte pointer;

        entryList = new EntryKeyList(buf);
        assertThrows(IllegalArgumentException.class, () -> {
            entryList.pointer(4, (byte)-4);
        });
    }

    @Test
    public void testWriteKey() {
        ByteBuffer buf = ByteBuffer.allocate(8 * 10);
        byte[] expectedKey = {0, 1, 2, 3, 4, 5, 6};
        byte[] key = new byte[7];

        entryList = new EntryKeyList(buf);
        entryList.key(4, expectedKey);
        key = entryList.key(4);

        assertArrayEquals(key, expectedKey);
        assertFalse(Arrays.equals(entryList.key(3), expectedKey));
    }

    @Test
    public void testWriteBadKey() {
        ByteBuffer buf = ByteBuffer.allocate(8 * 10);
        byte[] badKey1 = {0, 1, 2, 3, 4, 5, 6, 7};
        byte[] badKey2 = {0, 1, 2, 3, 4, 5};

        entryList = new EntryKeyList(buf);

        assertThrows(IllegalArgumentException.class, () -> {
            entryList.key(4, badKey1);
        });
        assertThrows(IllegalArgumentException.class, () -> {
            entryList.key(4, badKey2);
        });
    }

    @Test
    public void testWriteEntry() {
        ByteBuffer buf = ByteBuffer.allocate(8 * 10);
        byte[] key = {0, 1, 2, 3, 4, 5, 6};
        byte pointer = 34;
        boolean active = true;

        entryList = new EntryKeyList(buf);
        entryList.writeEntry(3, active, pointer, key);

        assertEquals(active, entryList.isActive(3));
        assertEquals(pointer, entryList.pointer(3));
        assertArrayEquals(key, entryList.key(3));
    }

    @Test
    public void testClear() {
        ByteBuffer buf = ByteBuffer.allocate(8 * 10);
        byte[] key = {0, 1, 2, 3, 4, 5, 6};
        byte pointer = 34;
        boolean active = true;

        entryList = new EntryKeyList(buf);
        entryList.writeEntry(3, active, pointer, key);
        entryList.clear();

        int entries = entryList.entries();
        for (int i=0; i < entries; i++) {
            assertEquals(false, entryList.isActive(i));
        }
    }
}
