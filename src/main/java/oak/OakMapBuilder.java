/**
 * Copyright 2018 Oath Inc.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package oak;

import java.nio.ByteBuffer;

/**
 * This class builds a new OakMap instance, and sets serializers, deserializers and allocation size calculators,
 * received from the user.
 *
 * @param <K> The key object type.
 * @param <V> The value object type.
 */
public class OakMapBuilder<K,V> {

  private Serializer<K> keySerializer;
  private Deserializer<K> keyDeserializer;
  private SizeCalculator<K> keySizeCalculator;
  private Serializer<V> valueSerializer;
  private Deserializer<V> valueDeserializer;
  private SizeCalculator<V> valueSizeCalculator;

  private K minKey;

  // comparators
  private OakComparator<K,K> keysComparator;
  private OakComparator<ByteBuffer,ByteBuffer> serializationsComparator;
  private OakComparator<ByteBuffer,K> serializationAndKeyComparator;

  // Off-heap fields
  private int chunkMaxItems;
  private int chunkBytesPerItem;
  private MemoryPool memoryPool;

  public OakMapBuilder() {
    this.keySerializer = null;
    this.keyDeserializer = null;
    this.keySizeCalculator = null;
    this.valueSerializer = null;
    this.valueDeserializer = null;
    this.valueSizeCalculator = null;

    this.minKey = null;

    this.keysComparator = null;
    this.serializationsComparator = null;
    this.serializationAndKeyComparator = null;

    this.chunkMaxItems = Chunk.MAX_ITEMS_DEFAULT;
    this.chunkBytesPerItem = Chunk.BYTES_PER_ITEM_DEFAULT;
    this.memoryPool = new SimpleNoFreeMemoryPoolImpl(Integer.MAX_VALUE);
  }

  public OakMapBuilder setKeySerializer(Serializer<K> keySerializer) {
    this.keySerializer = keySerializer;
    return this;
  }

  public OakMapBuilder setKeyDeserializer(Deserializer<K> keyDeserializer) {
    this.keyDeserializer = keyDeserializer;
    return this;
  }

  public OakMapBuilder setKeySizeCalculator(SizeCalculator<K> keySizeCalculator) {
    this.keySizeCalculator = keySizeCalculator;
    return this;
  }

  public OakMapBuilder setValueSerializer(Serializer<V> valueSerializer) {
    this.valueSerializer = valueSerializer;
    return this;
  }

  public OakMapBuilder setValueDeserializer(Deserializer<V> valueDeserializer) {
    this.valueDeserializer = valueDeserializer;
    return this;
  }

  public OakMapBuilder setValueSizeCalculator(SizeCalculator<V> valueSizeCalculator) {
    this.valueSizeCalculator = valueSizeCalculator;
    return this;
  }

  public OakMapBuilder setMinKey(K minKey) {
    this.minKey = minKey;
    return this;
  }

  public OakMapBuilder setChunkMaxItems(int chunkMaxItems) {
    this.chunkMaxItems = chunkMaxItems;
    return this;
  }

  public OakMapBuilder setChunkBytesPerItem(int chunkBytesPerItem) {
    this.chunkBytesPerItem = chunkBytesPerItem;
    return this;
  }

  public OakMapBuilder setMemoryPool(MemoryPool memoryPool) {
    this.memoryPool = memoryPool;
    return this;
  }

  public OakMapBuilder setKeysComparator(OakComparator<K,K> keysComparator) {
    this.keysComparator = keysComparator;
    return this;
  }

  public OakMapBuilder setSerializationsComparator(OakComparator<ByteBuffer,ByteBuffer> serializationsComparator) {
    this.serializationsComparator = serializationsComparator;
    return this;
  }

  public OakMapBuilder setSerializationAndKeyComparator(OakComparator<ByteBuffer,K> serializationAndKeyComparator) {
    this.serializationAndKeyComparator = serializationAndKeyComparator;
    return this;
  }

  public OakMapOffHeapImpl buildOffHeapOakMap() {

    assert this.keySerializer != null;
    assert this.keyDeserializer != null;
    assert this.keySizeCalculator != null;
    assert this.valueSerializer != null;
    assert this.valueDeserializer != null;
    assert this.valueSizeCalculator != null;
    assert this.minKey != null;
    assert this.keysComparator != null;
    assert this.serializationsComparator != null;
    assert this.serializationAndKeyComparator != null;

    return new OakMapOffHeapImpl(
            minKey,
            keySerializer,
            keyDeserializer,
            keySizeCalculator,
            valueSerializer,
            valueDeserializer,
            valueSizeCalculator,
            keysComparator,
            serializationsComparator,
            serializationAndKeyComparator,
            memoryPool,
            chunkMaxItems,
            chunkBytesPerItem);
  }

  private static int intsCompare(int int1, int int2) {
    if (int1 > int2)
      return 1;
    else if (int1 < int2)
      return -1;
    return 0;
  }

  public static OakMapBuilder<Integer, Integer> getDefaultBuilder() {

    Serializer<Integer> serializer = new Serializer<Integer>() {
      @Override
      public void serialize(Integer obj, ByteBuffer targetBuffer) {
        targetBuffer.putInt(targetBuffer.position(), obj);
      }
    };

    Deserializer<Integer> deserializer = new Deserializer<Integer>() {
      @Override
      public Integer deserialize(ByteBuffer byteBuffer) {
        return byteBuffer.getInt(byteBuffer.position());
      }
    };

    SizeCalculator<Integer> sizeCalculator = new SizeCalculator<Integer>() {
      @Override
      public int calculateSize(Integer object) {
        return Integer.BYTES;
      }
    };

    OakComparator<Integer, Integer> keysComparator = new OakComparator<Integer, Integer>() {
      @Override
      public int compare(Integer int1, Integer int2) {
        return intsCompare(int1, int2);
      }
    };

    OakComparator<ByteBuffer, ByteBuffer> serializationsComparator = new OakComparator<ByteBuffer, ByteBuffer>() {
      @Override
      public int compare(ByteBuffer buff1, ByteBuffer buff2) {
        int int1 = buff1.getInt(buff1.position());
        int int2 = buff2.getInt(buff2.position());
        return intsCompare(int1, int2);
      }
    };

    OakComparator<ByteBuffer, Integer> serializationAndKeyComparator = new OakComparator<ByteBuffer, Integer>() {
      @Override
      public int compare(ByteBuffer buff1, Integer int2) {
        int int1 = buff1.getInt(buff1.position());
        return intsCompare(int1, int2);
      }
    };

    return new OakMapBuilder<Integer, Integer>()
            .setKeySerializer(serializer)
            .setKeyDeserializer(deserializer)
            .setKeySizeCalculator(sizeCalculator)
            .setValueSerializer(serializer)
            .setValueDeserializer(deserializer)
            .setValueSizeCalculator(sizeCalculator)
            .setMinKey(new Integer(Integer.MIN_VALUE))
            .setKeysComparator(keysComparator)
            .setSerializationsComparator(serializationsComparator)
            .setSerializationAndKeyComparator(serializationAndKeyComparator);
  }
}