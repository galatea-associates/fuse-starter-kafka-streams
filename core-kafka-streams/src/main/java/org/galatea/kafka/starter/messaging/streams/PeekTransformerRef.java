package org.galatea.kafka.starter.messaging.streams;

import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.galatea.kafka.starter.messaging.streams.util.PeekAction;

@RequiredArgsConstructor
public class PeekTransformerRef<K, V> extends TransformerRef<K, V, K, V> {

  private final PeekAction<K, V> action;
  private final static AtomicLong lastAssignedPeekId = new AtomicLong(0);

  @Override
  public KeyValue<K, V> transform(K key, V value, ProcessorTaskContext<K, V, Object> context) {
    action.apply(key, value, context);
    return KeyValue.pair(key, value);
  }

  @Override
  public String named() {
    return "e-peek-" + lastAssignedPeekId.incrementAndGet();
  }
}
