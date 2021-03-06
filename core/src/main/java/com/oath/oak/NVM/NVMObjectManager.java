package com.oath.oak.NVM;

import static org.junit.Assert.assertTrue;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.ByteBuffer;
import java.lang.IllegalArgumentException;
import java.lang.IndexOutOfBoundsException;
import java.lang.ThreadLocal;

import com.oath.oak.OakMemoryAllocator;
import com.oath.oak.OakOutOfMemoryException;

class NVMObjectManager {
    final Path path;
    final FileChannel fc;
    final MappedByteBuffer mapping;
    final BoundedAtomicInteger position;
    private final ThreadLocal<ByteBuffer> mappingViewStorage =
        new ThreadLocal<ByteBuffer>() {
            @Override protected ByteBuffer initialValue() {
                return mapping.duplicate();
            }
        };

    NVMObjectManager(String path, int capacity) throws IOException {
        this.path = Paths.get(path);
        fc = FileChannel.open(this.path, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);
        mapping = fc.map(FileChannel.MapMode.READ_WRITE, 0, capacity);
        position = new BoundedAtomicInteger(mapping.capacity());
    }

    public NVMObject allocate(int size) throws OakOutOfMemoryException {
        int prefixSize = Integer.BYTES;
        int allocatedSize = size + prefixSize;
        int currentPosition;
        try {
            currentPosition = position.getAndAddBounded(allocatedSize);
        } catch (IllegalArgumentException e) {
            throw new OakOutOfMemoryException();
        }
        int endPosition = currentPosition + allocatedSize;
        ByteBuffer mappingView = mappingViewStorage.get();

        assertTrue(endPosition > 0);
        mappingView.limit(endPosition);
        mappingView.position(currentPosition + prefixSize);
        ByteBuffer newBuffer = mappingView.slice();
        mappingView.putInt(currentPosition, size);

        return new NVMObject(currentPosition, newBuffer);
    }

    public void flush() {
        synchronized (mapping) {
            mapping.force();
        }
    }

    public void free(int object) {
    }

    public ByteBuffer get(int pointer) throws IndexOutOfBoundsException {
        if (pointer >= mapping.capacity()) {
            throw new IndexOutOfBoundsException();
        }

        ByteBuffer readerMapping = mappingViewStorage.get();
        readerMapping.clear();
        int size = readerMapping.getInt(pointer);
        int startPosition = pointer + Integer.BYTES;
        int endPosition = startPosition + size;

        if (endPosition >= mapping.capacity()) {
            throw new IndexOutOfBoundsException();
        }

        readerMapping.limit(endPosition);
        readerMapping.position(startPosition);
        ByteBuffer objectBuffer = readerMapping.slice();

        return objectBuffer;
    }
}

