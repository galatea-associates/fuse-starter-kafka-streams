package org.galatea.kafka.starter.config;

import org.galatea.kafka.starter.messaging.BaseStreamingService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RuntimeConfig {

  @Bean
  CommandLineRunner startStreams(BaseStreamingService service) {
    return args -> service.start();
  }
}
