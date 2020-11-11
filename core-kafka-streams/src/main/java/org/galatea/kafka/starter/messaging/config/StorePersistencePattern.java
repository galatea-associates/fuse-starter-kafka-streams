package org.galatea.kafka.starter.messaging.config;

import java.util.regex.Pattern;
import lombok.Data;

@Data
public class StorePersistencePattern {

  private Pattern pattern;
  private StorePersistence persistence;

  public void setPattern(String pattern) {
    this.pattern = Pattern.compile(pattern);
  }
}
