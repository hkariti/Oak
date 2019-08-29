package com.oath.oak.synchrobench.maps;

import java.io.IOException;
import java.nio.ByteBuffer;
import com.oath.oak.synchrobench.contention.abstractions.CompositionalOakMap;
import java.util.Iterator;
import com.oath.oak.NVM.NVMSkipList;

public class NVMSkipListMap<K, V> implements CompositionalOakMap<K, V> {

    private NVMSkipList nvmMap;

    public NVMSkipListMap() throws IOException {
        nvmMap = new NVMSkipList();
    }

    @Override
    public boolean getOak(Object key) {
        return nvmMap.get((int)key) != null;
    }

    @Override
    public void putOak(Object key, Object value) {
        nvmMap.put((int) key, (ByteBuffer) value);
    }

    @Override
    public boolean putIfAbsentOak(Object key, Object value) {
		if (nvmMap.get((int)key) == null) {
			nvmMap.put((int)key, (ByteBuffer)value);
			return true;
		}
		return false;
    }

    @Override
    public void removeOak(Object key) {
        nvmMap.remove((int)key);
    }

    @Override
    public boolean computeIfPresentOak(K key) {
        return false;
    }

    @Override
    public void computeOak(K key) {

    }

    @Override
    public boolean ascendOak(K from, int length) {
		return nvmMap.ascend((Integer)from, length);
    }

    @Override
    public boolean descendOak(K from, int length) {
		return nvmMap.descend((Integer)from, length);
    }

    @Override
    public void clear() {
		nvmMap.clear();
    }

    @Override
    public int size() {
        return nvmMap.size();
    }
}
