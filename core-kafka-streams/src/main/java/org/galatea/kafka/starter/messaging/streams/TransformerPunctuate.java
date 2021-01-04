package org.galatea.kafka.starter.messaging.streams;

import java.time.Duration;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.streams.processor.PunctuationType;
import org.galatea.kafka.starter.messaging.streams.util.TransformerPunctuateMethod;

@Getter
@Builder
@ToString
public class TransformerPunctuate<K,V,T> {

  private final TransformerPunctuateMethod<K,V,T> method;
  private final Duration interval;
  @Builder.Default
  private final PunctuationType type = PunctuationType.WALL_CLOCK_TIME;
}
