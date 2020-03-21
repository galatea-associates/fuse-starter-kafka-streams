package org.galatea.kafka.shell.domain;

import lombok.Data;

@Data
public class PartitionConsumptionStatus {

  private long latestOffsets = 0;
  private long consumedMessages = 0;
}
