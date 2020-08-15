package org.galatea.kafka.starter.messaging.streams;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.galatea.kafka.starter.messaging.streams.util.PeekAction;

@RequiredArgsConstructor
class PeekTransformer<K, V> implements ValueTransformerWithKey<K, V, V> {

  private final PeekAction<K,V> action;
  private TaskContext<Object> context;

  @Override
  public void init(ProcessorContext c) {
    context = new TaskContext<>(c, null);
  }

  @Override
  public V transform(K readOnlyKey, V value) {
    action.apply(readOnlyKey, value, context);
    return value;
  }

  @Override
  public void close() {

  }
}
