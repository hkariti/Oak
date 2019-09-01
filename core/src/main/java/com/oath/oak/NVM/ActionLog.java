package com.oath.oak.NVM;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.lang.IllegalArgumentException;

class ActionLog {
    static final int ENTRY_SIZE = Long.BYTES;
    static final int DELETED = 0;
    final LongBuffer logBuffer;
    final AtomicInteger next;
    final ArrayList<LongBuffer> writableEntries;

    public ActionLog(ByteBuffer buffer) {
        logBuffer = buffer.asLongBuffer();
        next = new BoundedAtomicInteger(logBuffer.capacity());
        writableEntries = getEntryList(logBuffer);
    }

    private ArrayList<LongBuffer> getEntryList(LongBuffer buffer) {
        ArrayList<LongBuffer> entries = new ArrayList<LongBuffer>(buffer.capacity());
        for (int i = 0; i < buffer.capacity(); i++) {
            buffer.position(i);
            buffer.limit(i+1);
            LongBuffer entry = buffer.slice();
            entries.add(entry);
        }

        return entries;
    }

    public int entries() {
        return logBuffer.capacity();
    }

    public void clear() {
        // This method is NOT thread-safe
        next.set(0);
        for (LongBuffer entry : writableEntries) {
            entry.put(0, 0);
        }
    }

    public ActionLogEntry get(int position) {
        long entry = writableEntries.get(position).duplicate().get(0);
        if (!isValid(entry)) {
            return null;
        }

        return new ActionLogEntry(pointer(entry), key(entry));
    }

    public int next() {
        return next.get();
    }

    public void next(int value) {
        synchronized (next) {
            next.set(value);
        }
    }

    public int put(int pointer, int key) throws IllegalArgumentException, IndexOutOfBoundsException {
        long entry;
        if (pointer < 0) {
            throw new IllegalArgumentException("pointer should be a positive integer");
        }
        int validAndPointer = (1 << 31) | pointer;
        entry = ((long)validAndPointer) << (4*8);
        entry |= key;

        int position = next.getAndIncrement();
        LongBuffer targetEntry = writableEntries.get(position);
        targetEntry.put(0, entry);

        return position;
    }

    private boolean isValid(long entry) {
        return (entry & (1L << 63)) != 0;
    }

    private int pointer(long entry) {
        int validAndPointer = (int)(entry >> (4*8));

        return validAndPointer & ~(1 << 31);
    }

    private int key(long entry) {
        return (int)(entry & ((1L << 32) - 1));
    }
}
