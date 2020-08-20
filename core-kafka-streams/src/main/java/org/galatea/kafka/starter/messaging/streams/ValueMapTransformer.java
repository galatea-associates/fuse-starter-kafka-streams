package org.galatea.kafka.starter.messaging.streams;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.galatea.kafka.starter.messaging.streams.util.ValueMapper;

@RequiredArgsConstructor
class ValueMapTransformer<K, V, V1> implements ValueTransformerWithKey<K, V, V1> {

  private final ValueMapper<K, V, V1> action;
  private TaskContext context;

  @Override
  public void init(ProcessorContext c) {
    context = new TaskContext(c);
  }

  @Override
  public V1 transform(K readOnlyKey, V value) {
    return action.apply(readOnlyKey, value, context);
  }

  @Override
  public void close() {

  }
}
