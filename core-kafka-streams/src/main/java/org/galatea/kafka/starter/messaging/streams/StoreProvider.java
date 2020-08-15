package org.galatea.kafka.starter.messaging.streams;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class StoreProvider {

  private final Map<String, TaskStore<?, ?>> taskStores = new ConcurrentHashMap<>();
  private final Map<String, GlobalStore<?, ?>> globalStores = new ConcurrentHashMap<>();
  private final ProcessorContext context;

  StoreProvider(ProcessorContext context) {
    this.context = context;
  }

  @SuppressWarnings("unchecked")
  public <K, V> TaskStore<K, V> store(TaskStoreRef<K, V> ref) {
    return (TaskStore<K, V>) taskStores.computeIfAbsent(ref.getName(),
        name -> {
          TaskContext taskContext = new TaskContext(context);
          return new TaskStore<>((KeyValueStore<K, V>) context.getStateStore(name),
              ref.getRetentionPolicy(), taskContext);
        });
  }

  @SuppressWarnings("unchecked")
  public <K, V> GlobalStore<K, V> store(GlobalStoreRef<K, V> ref) {
    return (GlobalStore<K, V>) globalStores.computeIfAbsent(ref.getName(),
        name -> new GlobalStore<>((KeyValueStore<?, ?>) context.getStateStore(name)));
  }
}
