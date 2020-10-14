package org.galatea.kafka.starter;

import lombok.Value;

@Value
public class Pair<K,V> {
  K key;
  V value;
}
