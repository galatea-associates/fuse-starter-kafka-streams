package org.galatea.kafka.starter.messaging.streams;

import java.util.Optional;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.streams.util.StoreUpdateCallback;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class GlobalStoreRef<K, V> extends StoreRef<K, V> {

  @Getter
  @NonNull
  private final Topic<K, V> onTopic;
  /**
   * Actions to take when this store is updated <strong>while the service is online</strong>. Due to
   * the nature of global stores, they cannot natively detect updates that occurred while the service
   * was offline.
   * If detecting updates that occurred while the service was offline is required, consider using a
   * TaskStore to track the latest known state of the global store, and a punctuate to compare task
   * stores against this global store.
   */
  private final StoreUpdateCallback<K, V> onUpdate;

  @Builder
  public GlobalStoreRef(String name, @NonNull Topic<K, V> onTopic,
      StoreUpdateCallback<K, V> onUpdate) {
    super(getName(name, onTopic), onTopic.getKeySerde(), onTopic.getValueSerde());
    this.onTopic = onTopic;
    this.onUpdate = Optional.ofNullable(onUpdate).orElse((key, oldValue, newValue) -> {
      // do nothing
    });
  }

  private static <K, V> String getName(String name, Topic<K, V> topic) {
    if (name == null) {
      name = "global-" + topic.getName();
    }
    return name;
  }
}
