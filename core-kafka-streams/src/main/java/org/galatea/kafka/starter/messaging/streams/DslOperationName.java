package org.galatea.kafka.starter.messaging.streams;

import lombok.Getter;

enum DslOperationName {
  PEEK("peek-"),
  MAP("map-"),
  MAP_VALUE("map-value-"),
  ;

  @Getter
  private final String prefix;
  DslOperationName(String prefix) {
    this.prefix = prefix;
  }
}
