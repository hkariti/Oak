import java.io.IOException;
import java.nio.ByteBuffer;

import com.oath.oak.MMAPAllocator;

public class AllocatorRunner {
    public static void main(String[] args) throws IOException {
        MMAPAllocator allocator = new MMAPAllocator(args[0], 1024);
        ByteBuffer buffer = allocator.allocate(128);
        buffer.put((byte)'a');
        buffer = allocator.allocate(128);
        buffer.put((byte)'a');
        buffer = allocator.allocate(128);
        buffer.put((byte)'a');
        buffer = allocator.allocate(128);
        buffer.put((byte)'a');
        buffer = allocator.allocate(128);
        buffer.put((byte)'a');
        buffer = allocator.allocate(128);
        buffer.put((byte)'a');
        buffer = allocator.allocate(128);
        buffer.put((byte)'a');
        buffer = allocator.allocate(128);
        buffer.put((byte)'a');
    }
}
