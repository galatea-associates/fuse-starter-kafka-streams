package org.galatea.kafka.starter.messaging.streams.util;

public interface ProcessorForwarder<K,V> {

  void forward(K key, V value);
}
