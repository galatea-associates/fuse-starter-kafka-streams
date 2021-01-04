package org.galatea.kafka.starter.messaging.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.galatea.kafka.starter.messaging.streams.util.StoreUpdateCallback;

@Slf4j
public class SimpleProcessor<K, V> implements Processor<K, V> {

  private final GlobalStoreRef<K, V> ref;
  private final StoreUpdateCallback<K, V> storeUpdateCallback;
  private KeyValueStore<K, V> store;
  private ProcessorContext context;

  public SimpleProcessor(GlobalStoreRef<K, V> ref) {
    this(ref, null);
  }

  public SimpleProcessor(GlobalStoreRef<K, V> ref, StoreUpdateCallback<K, V> storeUpdateCallback) {
    this.ref = ref;
    this.storeUpdateCallback = storeUpdateCallback;
  }

  @Override
  public void init(ProcessorContext context) {
    store = (KeyValueStore<K, V>) context.getStateStore(ref.getName());
    this.context = context;
  }

  @Override
  public void process(K key, V value) {
    boolean useCallback = storeUpdateCallback != null;
    V oldValue = null;
    if (useCallback) {
      oldValue = store.get(key);
    }
    log.debug("{} Updating store {} with Key {} Value {}", context.taskId(), store.name(), key,
        value);
    store.put(key, value);
    if (useCallback) {
      log.debug("{} Callback with Key {} oldValue {} newValue {}", context.taskId(), key, oldValue,
          value);
      storeUpdateCallback.onUpdate(key, oldValue, value);
    }
  }

  @Override
  public void close() {

  }
}
