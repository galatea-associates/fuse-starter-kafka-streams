package org.galatea.kafka.shell.domain;

import java.util.HashMap;
import java.util.Optional;

public class OffsetMap extends HashMap<TopicOffsetType, Long> {

  public OffsetMap(TopicOffsetType type, Long offset) {
    put(type, offset);
  }

  public OffsetMap() {
  }

  public OffsetMap add(OffsetMap other) {
    other.forEach((offsetType, offset) -> put(offsetType,
        Optional.ofNullable(get(offsetType)).orElse(0L) + offset));
    return this;
  }
}
