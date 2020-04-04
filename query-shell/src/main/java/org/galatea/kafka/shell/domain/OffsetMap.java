package org.galatea.kafka.shell.domain;

import java.util.HashMap;

public class OffsetMap extends HashMap<TopicOffsetType, Long> {

  public OffsetMap(TopicOffsetType type, Long offset) {
    put(type, offset);
  }

  public OffsetMap() {
  }

  @Override
  public Long get(Object key) {
    return containsKey(key) ? super.get(key) : 0L;
  }

  public OffsetMap add(OffsetMap other) {
    other.forEach((offsetType, offset) -> put(offsetType, get(offsetType) + offset));
    return this;
  }
}
