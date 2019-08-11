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

        ByteBuffer value = skipList.getOak(key);

        assertEquals(value, null);
    }

    @Test
    public void testPutNewKey() throws Exception {
        skipList = new NVMSkipList();
        skipList.clear();
        int key = 234;
        ByteBuffer value = ByteBuffer.wrap("asd".getBytes());

        skipList.putOak(key, value);
        ByteBuffer retValue = skipList.getOak(key);

        assertTrue(retValue.compareTo(value) == 0);
    }

    @Test
    public void testPutExistingKey() throws Exception {
        skipList = new NVMSkipList();
        skipList.clear();
        int key = 234;
        ByteBuffer value = ByteBuffer.wrap("asd".getBytes());
        ByteBuffer value2 = ByteBuffer.wrap("123".getBytes());

        skipList.putOak(key, value);
        skipList.putOak(key, value2);
        ByteBuffer retValue = skipList.getOak(key);

        assertTrue(retValue.compareTo(value2) == 0);
    }

    @Test
    public void testPutIfAbsentMissingKey() throws Exception {
        skipList = new NVMSkipList();
        skipList.clear();
        int key = 234;
        ByteBuffer value = ByteBuffer.wrap("asd".getBytes());

        boolean ret = skipList.putIfAbsentOak(key, value);
        ByteBuffer retValue = skipList.getOak(key);

        assertTrue(retValue.compareTo(value) == 0);
        assertTrue(ret);
    }

    @Test
    public void testPutIfAbsentExistingKey() throws Exception {
        skipList = new NVMSkipList();
        skipList.clear();
        int key = 234;
        ByteBuffer value = ByteBuffer.wrap("asd".getBytes());
        ByteBuffer value2 = ByteBuffer.wrap("123".getBytes());

        skipList.putOak(key, value);
        boolean ret = skipList.putIfAbsentOak(key, value2);
        ByteBuffer retValue = skipList.getOak(key);

        assertTrue(retValue.compareTo(value) == 0);
        assertFalse(ret);
    }

    @Test
    public void testRemoveExistingKey() throws Exception {
        skipList = new NVMSkipList();
        skipList.clear();
        int key = 234;
        ByteBuffer value = ByteBuffer.wrap("asd".getBytes());

        skipList.putOak(key, value);
        skipList.removeOak(key);
        ByteBuffer retValue = skipList.getOak(key);

        assertEquals(null, retValue);
    }

    @Test
    public void testRemoveUpdatedKey() throws Exception {
        skipList = new NVMSkipList();
        skipList.clear();
        int key = 234;
        ByteBuffer value = ByteBuffer.wrap("asd".getBytes());
        ByteBuffer value2 = ByteBuffer.wrap("123".getBytes());

        skipList.putOak(key, value);
        skipList.putOak(key, value2);
        skipList.removeOak(key);
        ByteBuffer retValue = skipList.getOak(key);

        assertEquals(null, retValue);
    }

    @Test
    public void testRemoveMissingKey() throws Exception {
        skipList = new NVMSkipList();
        skipList.clear();
        int key = 234;

        skipList.removeOak(key);
        ByteBuffer retValue = skipList.getOak(key);

        assertEquals(null, retValue);
    }
}

