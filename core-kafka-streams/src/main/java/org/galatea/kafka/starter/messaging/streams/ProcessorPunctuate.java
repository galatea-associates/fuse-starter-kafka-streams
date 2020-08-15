package org.galatea.kafka.starter.messaging.streams;

import java.time.Duration;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.streams.processor.PunctuationType;
import org.galatea.kafka.starter.messaging.streams.util.PunctuateMethod;

@Getter
@Builder
@ToString
public class ProcessorPunctuate<K,V,T> {

  private final PunctuateMethod<K,V,T> method;
  private final Duration interval;
  @Builder.Default
  private final PunctuationType type = PunctuationType.WALL_CLOCK_TIME;
}
