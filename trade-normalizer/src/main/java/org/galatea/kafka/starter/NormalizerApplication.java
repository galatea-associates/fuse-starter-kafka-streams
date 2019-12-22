package org.galatea.kafka.starter;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class NormalizerApplication {

  public static void main(String[] args) {
    SpringApplication.run(NormalizerApplication.class, args);
  }
}
