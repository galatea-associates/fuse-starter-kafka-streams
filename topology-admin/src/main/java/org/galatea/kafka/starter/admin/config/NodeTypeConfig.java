package org.galatea.kafka.starter.admin.config;

import java.util.Map;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "node.types")
@Data
public class NodeTypeConfig {

  private Map<String, String> colors;
  private Map<String, String> readTypeArrow;
}
