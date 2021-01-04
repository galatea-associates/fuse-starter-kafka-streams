package org.galatea.kafka.starter.messaging.streams.util;

import java.time.Instant;
import org.galatea.kafka.starter.messaging.streams.TaskContext;

public interface ProcessorPunctuateMethod<T> {

  void punctuate(Instant timestamp, TaskContext taskContext, T state);
}
