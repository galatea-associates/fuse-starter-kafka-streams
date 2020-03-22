package org.galatea.kafka.shell.domain;

import lombok.Value;

@Value
public final class DbRecord {

  private final MutableField<Long> recordTimestamp = new MutableField<>(null);
  private final MutableField<Integer> partition = new MutableField<>(null);
  private final MutableField<Long> offset = new MutableField<>(null);
  private final MutableField<String> stringValue = new MutableField<>(null);
}
