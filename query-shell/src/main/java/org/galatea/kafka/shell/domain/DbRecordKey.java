package org.galatea.kafka.shell.domain;

import lombok.Value;

@Value
public class DbRecordKey {

  private MutableField<Long> partition = new MutableField<>(-1L);
  private MutableField<Long> offset = new MutableField<>(-1L);
  private MutableField<String> stringKey = new MutableField<>(null);
}
