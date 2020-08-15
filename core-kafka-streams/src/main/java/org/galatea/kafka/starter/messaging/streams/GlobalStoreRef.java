package org.galatea.kafka.starter.messaging.streams;

import java.util.HashSet;
import java.util.Set;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.streams.exception.IllegalTopologyException;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class GlobalStoreRef<K, V> extends StoreRef<K, V> {

  private static final Set<String> usedNames = new HashSet<>();
  @Getter
  @NonNull
  private final Topic<K, V> onTopic;
  // TODO: add processor


  @Builder
  public GlobalStoreRef(String name, @NonNull Topic<K, V> onTopic) {
    super(getName(name, onTopic), onTopic.getKeySerde(), onTopic.getValueSerde());
    this.onTopic = onTopic;
  }

  private static <K, V> String getName(String name, Topic<K, V> topic) {
    if (name == null) {
      name = "global-" + topic.getName();
    }
    if (!usedNames.add(name)) {
      throw new IllegalTopologyException(
          String.format("Global store name '%s' can only be used for 1 store", name));
    }
    return name;
  }
}
