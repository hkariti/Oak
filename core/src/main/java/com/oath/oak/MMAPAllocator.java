package com.oath.oak;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.ByteBuffer;

import com.oath.oak.OakMemoryAllocator;
import com.oath.oak.OakOutOfMemoryException;

public class MMAPAllocator implements OakMemoryAllocator {
    final Path path;
    final FileChannel fc;
    final MappedByteBuffer mapping;

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
