package org.galatea.kafka.starter.messaging.streams.util;

import java.time.Instant;
import org.galatea.kafka.starter.messaging.streams.ProcessorTaskContext;

public interface PunctuateArgSupplier<K1, V1, T, U> {

  U arg(Instant timestamp, ProcessorTaskContext<K1, V1, T> taskContext);

  static <K1,V1,T, U> PunctuateArgSupplier<K1,V1,T, U> nullSupplier() {
    return (timestamp, taskContext) -> null;
  }
}
