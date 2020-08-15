package org.galatea.kafka.starter.messaging.streams;

import lombok.Getter;
import lombok.experimental.Delegate;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.galatea.kafka.starter.messaging.streams.util.ProcessorForwarder;

public class ProcessorTaskContext<K1, V1, T> extends TaskContext {

  @Delegate
  private final StoreProvider storeProvider;
  @Getter
  private final T state;
  @Delegate
  private final ProcessorForwarder<K1, V1> forwarder;

  ProcessorTaskContext(ProcessorContext innerContext, T state,
      ProcessorForwarder<K1, V1> forwarder) {
    super(innerContext);
    this.storeProvider = new StoreProvider(innerContext);
    this.state = state;
    this.forwarder = forwarder;
  }

}
