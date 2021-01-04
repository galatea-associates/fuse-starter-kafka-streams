package org.galatea.kafka.starter.messaging.streams;

import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
public class Range<T> {
  T from;
  T to;
}
