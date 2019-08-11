package com.oath.oak.NVM;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class NVMSkipList {
    class SkipListEntry {
        public final int mapEntry;
        public final ByteBuffer value;

        SkipListEntry(int mapEntry, ByteBuffer value) {
            this.mapEntry = mapEntry;
            this.value = value;
        }
    }
    static final int INIT_ENTRY_COUNT = 5;
    private ConcurrentSkipListMap<Integer, SkipListEntry> skipListMap;
    private NVMObjectManager objectManager;
    Comparator<Object> comparator;
    EntryKeyList recoveryMap;

    public NVMSkipList() throws IOException {
        skipListMap = new ConcurrentSkipListMap<>();
        objectManager = new NVMObjectManager("test.dat", Integer.MAX_VALUE);

        // Create the recovery map
        NVMObject mapObject = objectManager.allocate(INIT_ENTRY_COUNT * EntryKeyList.ENTRY_SIZE);
        recoveryMap = new EntryKeyList(mapObject.buffer);
    }

    public ByteBuffer getOak(Integer key) {
        SkipListEntry entry = skipListMap.get(key);
        if (entry == null) {
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

        synchronized (skipListMap) {
            SkipListEntry entry = skipListMap.get(key);
            int mapEntry;
            if (entry == null) {
                ByteBuffer keyBuffer = ByteBuffer.allocate(7);
                keyBuffer.putInt(key);
                mapEntry = recoveryMap.newEntry((byte)valuePointer, keyBuffer.array());
                objectManager.flush();
            } else {
                mapEntry = entry.mapEntry;
                recoveryMap.pointer(mapEntry, (byte)valuePointer);
                objectManager.flush();
            }
            entry = new SkipListEntry(mapEntry, value);
            skipListMap.put(key, entry);
        }
    }

    public boolean putIfAbsentOak(Integer key, ByteBuffer value) {
        synchronized (skipListMap) {
            SkipListEntry entry = skipListMap.get(key);
            if (entry != null) {
                return false;
            }
            NVMObject valueObject = objectManager.allocate(MyBufferOak.serializer.calculateSize(value));
            int valuePointer = valueObject.pointer;
            ByteBuffer valueBuffer = valueObject.buffer;
            MyBufferOak.serializer.serialize(value, valueBuffer);
            objectManager.flush();

            ByteBuffer keyBuffer = ByteBuffer.allocate(7);
            keyBuffer.putInt(key);
            int mapEntry = recoveryMap.newEntry((byte)valuePointer, keyBuffer.array());
            objectManager.flush();

            entry = new SkipListEntry(mapEntry, value);
            skipListMap.put(key, entry);

            return true;
        }
    }

    public void removeOak(Integer key) {
        synchronized (skipListMap) {
            SkipListEntry oldEntry = skipListMap.remove(key);
            if (oldEntry == null) {
                return;
            }
            recoveryMap.disable(oldEntry.mapEntry);
            objectManager.flush();
        }
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
        synchronized (skipListMap) {
            skipListMap.clear();
            recoveryMap.clear();
            objectManager.flush();
        }
    }

    public int size() {
        return skipListMap.size();
    }
}
