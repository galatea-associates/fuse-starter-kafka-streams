package org.galatea.kafka.shell.domain;

public enum ShellEntityType {
  TOPIC("topic"),
  STORE("store"),
  SCHEMA("schema"),
  CONSUMER_GROUP("consumer-group");

  private String value;

  ShellEntityType(String value) {
    this.value = value;
  }
}
