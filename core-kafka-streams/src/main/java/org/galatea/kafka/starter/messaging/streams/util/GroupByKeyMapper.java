package org.galatea.kafka.starter.messaging.streams.util;

public interface GroupByKeyMapper<K, K1, V> {

  K1 map(K key, V value);
}
