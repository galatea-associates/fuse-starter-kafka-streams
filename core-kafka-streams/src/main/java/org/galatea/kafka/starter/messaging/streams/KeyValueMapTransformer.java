package org.galatea.kafka.starter.messaging.streams;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.galatea.kafka.starter.messaging.streams.util.KeyValueMapper;

@RequiredArgsConstructor
class KeyValueMapTransformer<K, V, K1, V1> implements Transformer<K, V, KeyValue<K1,V1>> {

  private final KeyValueMapper<K, V, K1, V1> action;
  private TaskContext context;

  @Override
  public void init(ProcessorContext c) {
    context = new TaskContext(c);
  }

  @Override
  public KeyValue<K1,V1> transform(K key, V value) {
    return action.apply(key, value, context);
  }

  @Override
  public void close() {

  }
}
