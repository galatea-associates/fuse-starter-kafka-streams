package org.galatea.kafka.shell.domain;

import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
@RequiredArgsConstructor
public class TopicPartitionOffsets {

  private long beginningOffset;
  private long endOffset;
}
