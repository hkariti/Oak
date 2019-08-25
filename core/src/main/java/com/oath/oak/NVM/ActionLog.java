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
    // Have a global, synchronized list of entries for writing and a thread-local list for reading.
    // This way we can scan the log in parallel.
    final ArrayList<LongBuffer> writableEntries;
    ThreadLocal<ArrayList<LongBuffer>> readonlyEntriesStorage = new ThreadLocal<ArrayList<LongBuffer>>() {
        @Override protected ArrayList<LongBuffer> initialValue() {
            LongBuffer logBufferView = logBuffer.duplicate();
            return getEntryList(logBufferView);
        }
    };

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

    public void perThreadInit() {
        readonlyEntriesStorage.get();
    }

    public int entries() {
        return logBuffer.capacity();
    }

    public ActionLogEntry get(int position) {
        long entry = readonlyEntriesStorage.get().get(position).get(0);
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
        ArrayList<LongBuffer> readonlyEntries = readonlyEntriesStorage.get();
        int validAndPointer = (1 << 31) | pointer;
        entry = ((long)validAndPointer) << (4*8);
        entry |= key;

        int position = next.get();
        boolean written = false;
        long oldEntry;
        while (!written) {
            oldEntry = readonlyEntries.get(position).get(0);
            if (isValid(oldEntry)) {
                position++;
                continue;
            }
            written = compareAndSetEntry(position, oldEntry, entry);
            if (!written) {
                position++;
            }
        }
        writeMaxNext(position + 1);

        return position;
    }

    private boolean compareAndSetEntry(int position, long oldEntry, long newEntry) {
        LongBuffer entryBuffer = writableEntries.get(position);
        synchronized (entryBuffer) {
            long currentEntry = entryBuffer.get(0);
            if (oldEntry == currentEntry) {
                entryBuffer.put(0, newEntry);
                return true;
            }
            return false;
        }
    }

    private void writeMaxNext(int newPosition) {
        synchronized (next) {
            if (next.get() < newPosition) {
                next.set(newPosition);
            }
        }
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
