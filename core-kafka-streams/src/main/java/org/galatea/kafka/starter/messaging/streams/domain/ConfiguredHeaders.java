package org.galatea.kafka.starter.messaging.streams.domain;

import lombok.Getter;

public enum ConfiguredHeaders {
  NEW_PARTITION_KEY("PART_KEY"),
  USED_PARTITION_KEY("PART_KEY_USED");

  @Getter
  private final String key;
  private ConfiguredHeaders(String headerKey) {
    this.key = headerKey;
  }
}
