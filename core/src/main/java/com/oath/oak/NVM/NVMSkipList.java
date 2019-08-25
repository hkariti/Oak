package com.oath.oak.NVM;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class NVMSkipList {
    class SkipListEntry {
        public int logEntry;
        public ByteBuffer value;

        SkipListEntry(int logEntry, ByteBuffer value) {
            this.logEntry = logEntry;
            this.value = value;
        }

        synchronized void writeMax(int newLogEntry, ByteBuffer newValue) {
            if (newLogEntry <= logEntry)  {
                return;
            }
            logEntry = newLogEntry;
            value = newValue;
        }
    }
    static final int INIT_ENTRY_COUNT = 1024;
    private final ConcurrentSkipListMap<Integer, SkipListEntry> skipListMap;
    private final NVMObjectManager objectManager;
    private final ActionLog actionLog;

    public NVMSkipList() throws IOException {
        skipListMap = new ConcurrentSkipListMap<>();
        objectManager = new NVMObjectManager("test.dat", Integer.MAX_VALUE);

        // Create the recovery map
        NVMObject logObject = objectManager.allocate(INIT_ENTRY_COUNT * ActionLog.ENTRY_SIZE);
        actionLog = new ActionLog(logObject.buffer);
    }

    public ByteBuffer getOak(Integer key) {
        SkipListEntry entry = skipListMap.get(key);
        if (entry == null || entry.value == null) {
            return null;
        }

        ByteBuffer serializedValue = entry.value;

        return MyBufferOak.serializer.deserialize(serializedValue);
    }

    public void putOak(Integer key, ByteBuffer value) {
        NVMObject valueObject = objectManager.allocate(MyBufferOak.serializer.calculateSize(value));
        int valuePointer = valueObject.pointer;
        ByteBuffer valueBuffer = valueObject.buffer;
        MyBufferOak.serializer.serialize(value, valueBuffer);
        objectManager.flush();

        int logEntryNumber = actionLog.put(valuePointer, key);
        objectManager.flush();

        SkipListEntry indexEntry = new SkipListEntry(logEntryNumber, value);
        writeMaxSkipListEntry(key, indexEntry);
    }

    public boolean putIfAbsentOak(Integer key, ByteBuffer value) {
        return false;
    }

    public void removeOak(Integer key) {
        int logEntryNumber = actionLog.put(ActionLog.DELETED, key);
        objectManager.flush();

        SkipListEntry indexEntry = new SkipListEntry(logEntryNumber, null);
        writeMaxSkipListEntry(key, indexEntry);
    }

    private void writeMaxSkipListEntry(int key, SkipListEntry entry) {
        SkipListEntry existingEntry = skipListMap.putIfAbsent(key, entry);
        if (existingEntry == null) {
            return;
        }

        existingEntry.writeMax(entry.logEntry, entry.value);
    }

    public boolean computeIfPresentOak(Integer key) {
        return false;
    }

    public void computeOak(Integer key) {

    }

    public boolean ascendOak(Integer from, int length) {
        Iterator iter = skipListMap.tailMap(from, true).keySet().iterator();
        int i = 0;
        while (iter.hasNext() && i < length) {
            i++;
            iter.next();
        }
        return i == length;
    }

    public boolean descendOak(Integer from, int length) {
        Iterator iter = skipListMap.descendingMap().tailMap(from, true).keySet().iterator();
        int i = 0;
        while (iter.hasNext() && i < length) {
            i++;
            iter.next();
        }
        return i == length;
    }

    public void clear() {
        // TODO: Fix
        synchronized (skipListMap) {
            skipListMap.clear();
            objectManager.flush();
        }
    }

    public int size() {
        return skipListMap.size();
    }
}
