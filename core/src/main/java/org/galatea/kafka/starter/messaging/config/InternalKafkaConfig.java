package org.galatea.kafka.starter.messaging.config;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;

@Getter
public class InternalKafkaConfig {

  private final Map<String, String> global = new HashMap<>();
  private final Map<String, String> streams = new HashMap<>();
  private final Map<String, String> consumer = new HashMap<>();
  private final Map<String, String> producer = new HashMap<>();
  private final Map<String, String> admin = new HashMap<>();
}
