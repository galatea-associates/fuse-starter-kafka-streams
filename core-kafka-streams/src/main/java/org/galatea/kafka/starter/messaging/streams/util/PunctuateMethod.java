package org.galatea.kafka.starter.messaging.streams.util;

import java.time.Instant;
import org.galatea.kafka.starter.messaging.streams.ProcessorTaskContext;

public interface PunctuateMethod<K1, V1, T> {

  void punctuate(Instant timestamp, ProcessorTaskContext<K1, V1, T> taskContext);
}
