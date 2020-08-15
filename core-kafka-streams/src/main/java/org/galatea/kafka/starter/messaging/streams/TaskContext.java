package org.galatea.kafka.starter.messaging.streams;

import lombok.experimental.Delegate;
import org.apache.kafka.streams.processor.ProcessorContext;

public class TaskContext {

  @Delegate(excludes = ProcessorContextDelegateExcludes.class)
  private final ProcessorContext innerContext;

  private interface ProcessorContextDelegateExcludes {
    <K,V> void forward(K key, V value);
  }

  public TaskContext(ProcessorContext innerContext) {
    this.innerContext = innerContext;
  }
}
