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

public class NVMSkipListTest {
    static NVMSkipList skipList;

    @Test
    public void testGetNonExistingKey() throws Exception {
        skipList = new NVMSkipList();
        skipList.clear();
        int key = 123;

        ByteBuffer value = skipList.get(key);

        assertEquals(value, null);
    }

    @Test
    public void testPutNewKey() throws Exception {
        skipList = new NVMSkipList();
        skipList.clear();
        int key = 234;
        ByteBuffer value = ByteBuffer.wrap("asd".getBytes());

        skipList.put(key, value);
        ByteBuffer retValue = skipList.get(key);

        assertTrue(retValue.compareTo(value) == 0);
    }

    @Test
    public void testPutExistingKey() throws Exception {
        skipList = new NVMSkipList();
        skipList.clear();
        int key = 234;
        ByteBuffer value = ByteBuffer.wrap("asd".getBytes());
        ByteBuffer value2 = ByteBuffer.wrap("123".getBytes());

        skipList.put(key, value);
        skipList.put(key, value2);
        ByteBuffer retValue = skipList.get(key);

        assertTrue(retValue.compareTo(value2) == 0);
    }

    @Test
    public void testRemoveExistingKey() throws Exception {
        skipList = new NVMSkipList();
        skipList.clear();
        int key = 234;
        ByteBuffer value = ByteBuffer.wrap("asd".getBytes());

        skipList.put(key, value);
        skipList.remove(key);
        ByteBuffer retValue = skipList.get(key);

        assertEquals(null, retValue);
    }

    @Test
    public void testRemoveUpdatedKey() throws Exception {
        skipList = new NVMSkipList();
        skipList.clear();
        int key = 234;
        ByteBuffer value = ByteBuffer.wrap("asd".getBytes());
        ByteBuffer value2 = ByteBuffer.wrap("123".getBytes());

        skipList.put(key, value);
        skipList.put(key, value2);
        skipList.remove(key);
        ByteBuffer retValue = skipList.get(key);

        assertEquals(null, retValue);
    }

    @Test
    public void testRemoveMissingKey() throws Exception {
        skipList = new NVMSkipList();
        skipList.clear();
        int key = 234;

        skipList.remove(key);
        ByteBuffer retValue = skipList.get(key);

        assertEquals(null, retValue);
    }
}

