package org.galatea.kafka.shell.domain;

import lombok.Value;

@Value
public class RecordMetadata {

  private long offset;
  private int partition;
  private long timestamp;
  private String topic;
}