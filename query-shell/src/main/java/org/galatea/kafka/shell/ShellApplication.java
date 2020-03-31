package org.galatea.kafka.shell;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@Slf4j
@SpringBootApplication
@RequiredArgsConstructor
@EnableScheduling
public class ShellApplication {

  public static void main(String[] args) {
    SpringApplication.run(ShellApplication.class, args);
  }

}
