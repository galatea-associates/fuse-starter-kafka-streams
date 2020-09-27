package org.galatea.kafka.starter.messaging.streams;

import java.util.Iterator;
import lombok.experimental.Delegate;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.galatea.kafka.starter.messaging.streams.domain.ConfiguredHeaders;

public class TaskContext {

  @Delegate(excludes = ProcessorContextDelegateExcludes.class)
  private final ProcessorContext innerContext;

  public String partitionKey() {
    Iterator<Header> iter = innerContext.headers()
        .headers(ConfiguredHeaders.USED_PARTITION_KEY.getKey()).iterator();
    if (iter.hasNext()) {
      return new String(iter.next().value());
    }
    return null;
  }

  private interface ProcessorContextDelegateExcludes {

    <K, V> void forward(K key, V value);
  }

  public TaskContext(ProcessorContext innerContext) {
    this.innerContext = innerContext;
  }
}
