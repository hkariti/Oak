package com.oath.oak.NVM;

import java.util.concurrent.atomic.AtomicInteger;
import java.lang.IllegalArgumentException;
import java.io.NotSerializableException;

@SuppressWarnings("serial")
class BoundedAtomicInteger extends AtomicInteger {
    final int upperLimit;

    BoundedAtomicInteger(int upper) {
        super();
        upperLimit = upper;
    }

    final int getAndAddBounded(int delta) {
        for (;;) {
            int current = get();
            int next = current + delta;
            if (next > upperLimit) {
                throw new IllegalArgumentException("New value is above limit");
            }
            if (compareAndSet(current, next))
                return current;
        }
    }

    private void writeObject(java.io.ObjectOutputStream stream) throws NotSerializableException {
        throw new NotSerializableException();
    }
}
