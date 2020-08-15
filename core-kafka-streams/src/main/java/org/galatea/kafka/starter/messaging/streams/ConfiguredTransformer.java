package org.galatea.kafka.starter.messaging.streams;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

@RequiredArgsConstructor
class ConfiguredTransformer<K, V, K1, V1, T> implements Transformer<K, V, KeyValue<K1, V1>> {

  private final TransformerRef<K, V, K1, V1, T> transformerRef;
  private TaskContext<T> context;

  @Override
  public void init(ProcessorContext context) {
    T state = transformerRef.initState();
    this.context = new TaskContext<>(context, state);
  }

  @Override
  public KeyValue<K1, V1> transform(K key, V value) {
    return transformerRef.transform(key, value, context);
  }

  @Override
  public void close() {
    transformerRef.close(context);
  }

}
