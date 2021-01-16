package org.galatea.kafka.starter.util;

import lombok.Getter;
import lombok.Value;

@Getter
@Value(staticConstructor = "of")
public class Pair<K,V> {
  K key;
  V value;
}
