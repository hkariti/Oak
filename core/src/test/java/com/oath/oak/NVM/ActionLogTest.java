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

public class ActionLogTest {
    static ActionLog actionLog;

    @Test
    public void testConstructor() {
        ByteBuffer buf = ByteBuffer.allocate(ActionLog.ENTRY_SIZE * 10);

        actionLog = new ActionLog(buf);

        assertEquals(actionLog.entries(), 10);
        assertEquals(actionLog.next(), 0);
    }

    @Test
    public void testGetMissingEntry() {
        ByteBuffer buf = ByteBuffer.allocate(ActionLog.ENTRY_SIZE * 10);

        actionLog = new ActionLog(buf);

        assertEquals(actionLog.get(0), null);
    }

    @Test
    public void testAddEntry() {
        ByteBuffer buf = ByteBuffer.allocate(ActionLog.ENTRY_SIZE * 10);

        actionLog = new ActionLog(buf);
        int pointer = 23;
        int key = 123;
        int position = actionLog.put(pointer, key);
        ActionLogEntry entry = actionLog.get(0);

        assertEquals(position, 0);
        assertNotEquals(entry, null);
        assertEquals(entry.pointer, pointer);
        assertEquals(entry.key, key);
        assertEquals(actionLog.next(), 1);
    }

    @Test
    public void testFillLog() {
        ByteBuffer buf = ByteBuffer.allocate(ActionLog.ENTRY_SIZE * 10);

        actionLog = new ActionLog(buf);

        int pointer, key, position;
        ActionLogEntry entry;
        for (int i=0; i < 10; i++) {
            pointer = 100 + i;
            key = 200 + i;
            position = actionLog.put(pointer, key);
            entry = actionLog.get(i);

            assertEquals(position, i);
            assertNotEquals(entry, null);
            assertEquals(entry.pointer, pointer);
            assertEquals(entry.key, key);
            assertEquals(actionLog.next(), i+1);
        }

        // Try to overflow the log
        assertThrows(IndexOutOfBoundsException.class,  () -> actionLog.put(123, 567));
    }

    @Test
    public void testAddEntryWithNegativePointer() {
        ByteBuffer buf = ByteBuffer.allocate(ActionLog.ENTRY_SIZE * 10);

        actionLog = new ActionLog(buf);

        assertThrows(IllegalArgumentException.class, () -> actionLog.put(-123, 567));
    }

    @Test
    public void testAddEntryDoesNotOverrideExisting() {
        ByteBuffer buf = ByteBuffer.allocate(ActionLog.ENTRY_SIZE * 10);

        actionLog = new ActionLog(buf);
        // Simulate usecase where next pointers to a valid entry
        actionLog.put(1, 1);
        actionLog.next(0);

        int position = actionLog.put(2, 2);

        ActionLogEntry firstEntry = actionLog.get(0);
        ActionLogEntry secondEntry = actionLog.get(1);

        assertEquals(position, 1);
        assertEquals(actionLog.next(), 2);
        assertEquals(firstEntry.key, 1);
        assertEquals(firstEntry.pointer, 1);
        assertEquals(secondEntry.key, 2);
        assertEquals(secondEntry.pointer, 2);
    }
}
