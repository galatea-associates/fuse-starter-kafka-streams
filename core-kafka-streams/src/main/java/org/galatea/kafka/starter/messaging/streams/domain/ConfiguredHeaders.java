package org.galatea.kafka.starter.messaging.streams.domain;

import lombok.Getter;

public enum ConfiguredHeaders {
  /**
   * Header for partition key to be used when the record is repartitioned
   */
  NEW_PARTITION_KEY("PART_KEY"),
  /**
   * Header for partition key that was used for this record
   */
  USED_PARTITION_KEY("PART_KEY_USED");

  @Getter
  private final String key;

  ConfiguredHeaders(String headerKey) {
    this.key = headerKey;
  }
}
