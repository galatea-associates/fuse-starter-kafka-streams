package org.galatea.kafka.starter.messaging.streams.domain;

import lombok.Getter;

public enum ConfiguredHeaders {
  PARTITION_KEY("PART_KEY");

  @Getter
  private final String key;
  private ConfiguredHeaders(String headerKey) {
    this.key = headerKey;
  }
}
