package org.galatea.kafka.starter.messaging.streams;

import lombok.experimental.Delegate;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;

public class TaskContext {

  @Delegate(excludes = ProcessorContextDelegateExcludes.class)
  private final ProcessorContext innerContext;

  private interface ProcessorContextDelegateExcludes {

    <K, V> void forward(K key, V value);

    <K, V> void forward(K key, V value, To to);

    <K, V> void forward(K key, V value, int to);

    <K, V> void forward(K key, V value, String to);
  }

  public TaskContext(ProcessorContext innerContext) {
    this.innerContext = innerContext;
  }
}
