package org.galatea.kafka.starter.messaging.streams;

import lombok.Getter;
import lombok.experimental.Delegate;
import org.apache.kafka.streams.processor.ProcessorContext;

public class TaskContext<T> {

  @Delegate
  private final ProcessorContext innerContext;
  @Getter
  private final T state;

  public TaskContext(ProcessorContext innerContext, T state) {
    this.innerContext = innerContext;
    this.state = state;
    storeProvider = new StoreProvider(innerContext);
  }

  @Delegate
  private final StoreProvider storeProvider;
}
