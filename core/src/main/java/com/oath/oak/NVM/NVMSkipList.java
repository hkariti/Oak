package com.oath.oak.NVM;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class NVMSkipList {
    static final int INIT_ENTRY_COUNT = 5;
    private ConcurrentSkipListMap<Integer, Integer> skipListMap;
    private NVMObjectManager objectManager;
    Comparator<Object> comparator;
    EntryKeyList recoveryMap;

    public NVMSkipList() throws IOException {
        skipListMap = new ConcurrentSkipListMap<>();
        objectManager = new NVMObjectManager("test.dat", Integer.MAX_VALUE);

        // Create the recovery map
        objectManager.allocate(INIT_ENTRY_COUNT * EntryKeyList.ENTRY_SIZE);
        recoveryMap = new EntryKeyList(objectManager.get(0));
    }

    public ByteBuffer getOak(Integer key) {
        Integer mapEntry = skipListMap.get(key);
        if (mapEntry == null) {
            return null;
        }

        int valuePointer = recoveryMap.pointer(mapEntry);
        ByteBuffer serializedValue = objectManager.get(valuePointer);

        return MyBufferOak.serializer.deserialize(serializedValue);
    }

    public void putOak(Integer key, ByteBuffer value) {
        int valuePointer = objectManager.allocate(MyBufferOak.serializer.calculateSize(value));
        ByteBuffer valueBuffer = objectManager.get(valuePointer);
        MyBufferOak.serializer.serialize(value, valueBuffer);
        objectManager.flush();

        Integer mapEntry = skipListMap.get(key);
        if (mapEntry == null) {
            ByteBuffer keyBuffer = ByteBuffer.allocate(7);
            keyBuffer.putInt(key);
            mapEntry = recoveryMap.newEntry((byte)valuePointer, keyBuffer.array());
            objectManager.flush();
            skipListMap.put(key, mapEntry);
        } else {
            recoveryMap.pointer(mapEntry, (byte)valuePointer);
            objectManager.flush();
        }
    }

    public boolean putIfAbsentOak(Integer key, ByteBuffer value) {
        if (skipListMap.containsKey(key)) {
            return false;
        }
        putOak(key, value);
        return true;
    }

    public void removeOak(Integer key) {
        Integer mapEntry = skipListMap.get(key);
        if (mapEntry == null) {
            return;
        }
        recoveryMap.disable(mapEntry);
        objectManager.flush();
        skipListMap.remove(key);
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
        recoveryMap.clear();
        objectManager.flush();
    }

    public int size() {
        return skipListMap.size();
    }
}
