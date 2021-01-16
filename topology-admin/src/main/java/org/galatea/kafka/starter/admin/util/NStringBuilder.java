package org.galatea.kafka.starter.admin.util;

public class NStringBuilder {

  private final StringBuilder inner = new StringBuilder();

  public String toString() {
    return inner.toString();
  }

  public NStringBuilder append(String message, Object... args) {
    inner.append(String.format(message, args));

    return this;
  }

}
