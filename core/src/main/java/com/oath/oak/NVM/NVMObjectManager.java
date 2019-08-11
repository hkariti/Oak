package com.oath.oak.NVM;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.ByteBuffer;
import java.lang.IllegalArgumentException;

import com.oath.oak.OakMemoryAllocator;
import com.oath.oak.OakOutOfMemoryException;

class NVMObjectManager {
    MappedByteBuffer mapping;
    MMAPAllocator allocator;
    int allocatedCount = 0;

    class MMAPAllocator implements OakMemoryAllocator {
        final Path path;
        final FileChannel fc;

        public ByteBuffer allocate(int size) {
            int currentPosition = mapping.position();
            int endPosition = currentPosition + size;

            try {
                mapping.limit(endPosition);
            } catch (IllegalArgumentException e) {
                throw new OakOutOfMemoryException();
            }
            ByteBuffer newBuffer = mapping.slice();
            mapping.limit(mapping.capacity());
            mapping.position(endPosition);
            return newBuffer;
        }

        public void flush() {
            mapping.force();
        }

        public void free(ByteBuffer bb) {
        }

        public void close() {
        }

        public long allocated() {
            return mapping.position();
        }

        public MMAPAllocator(String path, int capacity) throws IOException {
            this.path = Paths.get(path);
            fc = FileChannel.open(this.path, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);
            mapping = fc.map(FileChannel.MapMode.READ_WRITE, 0, capacity);
        }
    }

    NVMObjectManager(String path, int capacity) throws IOException {
        allocator = new MMAPAllocator(path, capacity);
    }
    public NVMObject allocate(int size) {
        synchronized (mapping) {
            ByteBuffer buffer = allocator.allocate(size + Long.BYTES);
            buffer.putInt(0, size);
            int pointer = allocatedCount;
            allocatedCount++;
            return new NVMObject(pointer, buffer);
        }
    }

    public void flush() {
        allocator.flush();
    }

    public void free(int object) {
    }

    public ByteBuffer get(int object) throws IllegalArgumentException {
        if (object >= allocatedCount) {
            throw new IllegalArgumentException("No such object");
        }

        int offset = 0;
        for (int i=0; i < object; i++) {
            offset += mapping.getInt(offset) + Long.BYTES;
        }

        int size = mapping.getInt(offset);
        ByteBuffer readerMapping = mapping.duplicate();
        readerMapping.position(offset + Long.BYTES);
        readerMapping.limit(offset + Long.BYTES + size);
        ByteBuffer objectBuffer = readerMapping.slice();

        return objectBuffer;
    }

    public long allocated() {
        return allocatedCount;
    }
}

