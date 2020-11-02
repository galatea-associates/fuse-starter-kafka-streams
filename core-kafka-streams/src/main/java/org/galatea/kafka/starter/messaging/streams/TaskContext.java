package org.galatea.kafka.starter.messaging.streams;

import java.util.Iterator;
import lombok.experimental.Delegate;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.galatea.kafka.starter.messaging.streams.domain.ConfiguredHeaders;

public class TaskContext {

  @Delegate(excludes = ProcessorContextDelegateExcludes.class)
  private final ProcessorContext innerContext;

  public byte[] partitionKey() {

    Iterator<Header> iter = innerContext.headers()
        .headers(ConfiguredHeaders.USED_PARTITION_KEY.getKey()).iterator();
    if (iter.hasNext()) {
      return iter.next().value();
    }
    return null;
  }

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
