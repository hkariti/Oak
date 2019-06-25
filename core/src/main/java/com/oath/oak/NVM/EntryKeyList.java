package com.oath.oak.NVM;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.lang.IllegalArgumentException;

class EntryKeyList {
    ByteBuffer buffer;
    static final int ENTRY_SIZE = 8;
    int entries;

    public EntryKeyList(ByteBuffer buffer) {
        this.buffer = buffer;
        entries = buffer.capacity() / ENTRY_SIZE;
    }

    int entries() {
        return entries;
    }

    boolean isActive(int entry) {
        ByteBuffer entryBuffer = getEntry(entry);
        short firstByte = entryBuffer.get(0);

        // Make sure value is of unsigned byte
        if (firstByte < 0) {
            firstByte += 256;
        }
        return (firstByte >> 7) == 1;
    }

    byte pointer(int entry) {
        ByteBuffer entryBuffer = getEntry(entry);
        byte firstByte = entryBuffer.get(0);
        return (byte)(firstByte & 0b01111111);
    }

    void pointer(int entry, byte newPointer) throws IllegalArgumentException {
        if (newPointer < 0) {
            throw new IllegalArgumentException("pointer must be >0 and <128");
        }

        ByteBuffer entryBuffer = getEntry(entry);
        byte firstByte = entryBuffer.get(0);

        firstByte &= 0b10000000;
        firstByte |= newPointer;
        entryBuffer.put(0, firstByte);
    }

    byte[] key(int entry) {
        ByteBuffer entryBuffer = getEntry(entry);
        byte[] key = new byte[7];

        entryBuffer.position(1);
        entryBuffer.get(key);
        return key;
    }

    void key(int entry, byte[] newKey) throws IllegalArgumentException {
        if (newKey.length != 7) {
            throw new IllegalArgumentException("key must be exactly 7 bytes long");
        }

        ByteBuffer entryBuffer = getEntry(entry);

        entryBuffer.position(1);
        entryBuffer.put(newKey);
    }

    void disable(int entry) {
        if (isActive(entry)) {
            ByteBuffer entryBuffer = getEntry(entry);
            byte firstByte = entryBuffer.get(0);
            byte newByte = (byte)(firstByte & 0b01111111);
            entryBuffer.put(0, newByte);
        }
    }

    void enable(int entry) {
        if (!isActive(entry)) {
            ByteBuffer entryBuffer = getEntry(entry);
            byte firstByte = entryBuffer.get(0);
            byte newByte = (byte)(firstByte | 0b10000000);
            entryBuffer.put(0, newByte);
        }
    }

    int newEntry(byte pointer, byte[] key) throws RuntimeException {
        synchronized (buffer) {
            for (int i=0; i < entries; i++) {
                if (!isActive(i)) {
                    writeEntry(i, true, pointer, key);
                    return i;
                }
            }
        }
        throw new RuntimeException("Map is full");
    }

    void writeEntry(int entry, boolean active, byte pointer, byte[] key) throws IllegalArgumentException {
        if (key.length != 7) {
            throw new IllegalArgumentException("key must be exactly 7 bytes long");
        }
        if (pointer < 0) {
            throw new IllegalArgumentException("pointer must be between 0 and 127");
        }
        ByteBuffer entryBuffer = getEntry(entry);
        byte activePointer = (byte)((active ? 1 : 0) << 7 | pointer);
        long entryData = (long)activePointer << (8 * 7);
        for (int i=0; i < key.length; i++) {
            entryData |= ((long)key[i] & 0xffL) << (8 * (7 - (i+1)));
        }
        entryBuffer.putLong(0, entryData);
    }

    void clear() {
        for (int i=0; i < entries; i++) {
            disable(i);
        }
    }

    private ByteBuffer getEntry(int entry) throws IllegalArgumentException {
        if (entry > entries()) {
            throw new IllegalArgumentException("no such entry");
        }

        int startPosition = entry * ENTRY_SIZE;
        buffer.position(startPosition);
        ByteBuffer entryBuffer = buffer.slice();
        entryBuffer.limit(ENTRY_SIZE);

        return entryBuffer;
    }
}
