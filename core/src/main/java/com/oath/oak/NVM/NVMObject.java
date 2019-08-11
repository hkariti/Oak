package com.oath.oak.NVM;

import java.nio.ByteBuffer;

class NVMObject {
    public final int pointer;
    public final ByteBuffer buffer;

    NVMObject(int pointer, ByteBuffer buffer) {
        this.pointer = pointer;
        this.buffer = buffer;
    }
}
