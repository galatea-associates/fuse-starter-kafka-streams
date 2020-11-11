package org.galatea.kafka.starter.messaging.streams.util;

public interface StoreUpdateCallback<K,V> {

  void onUpdate(K key, V oldValue, V newValue);
}
