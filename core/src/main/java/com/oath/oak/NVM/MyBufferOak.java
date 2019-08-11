package com.oath.oak.NVM;

import com.oath.oak.OakComparator;
import com.oath.oak.OakSerializer;

import java.nio.ByteBuffer;

class MyBufferOak {
    static OakSerializer<ByteBuffer> serializer = new OakSerializer<ByteBuffer>() {

        @Override
        public void serialize(ByteBuffer key, ByteBuffer targetBuffer) {
            int cap = key.capacity();
            int pos = targetBuffer.position();

            for (int i = 0; i < cap; i += 1) {
                targetBuffer.put(pos + i, key.get(i));
            }
        }

        @Override
        public ByteBuffer deserialize(ByteBuffer serializedKey) {
            int pos = serializedKey.position();
            int cap = serializedKey.capacity();
            ByteBuffer ret = ByteBuffer.allocate(cap);
            for (int i = 0; i < cap; i += 1) {
                ret.put(i, serializedKey.get(pos + i));
            }
            return ret;
        }

        @Override
        public int calculateSize(ByteBuffer object) {
            return object.capacity();
        }
    };
}
