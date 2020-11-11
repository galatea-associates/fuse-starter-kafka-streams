package org.galatea.kafka.starter.messaging.streams;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.common.serialization.Serde;
import org.galatea.kafka.starter.messaging.streams.util.ValidRecordPolicy;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class TaskStoreRef<K, V> extends StoreRef<K, V> {

    @Getter(value = AccessLevel.PACKAGE)
    private final ValidRecordPolicy<K, V> validRecordPolicy;

    @Builder
    public TaskStoreRef(String name, Serde<K> keySerde,
        Serde<V> valueSerde, ValidRecordPolicy<K, V> validRecordPolicy) {
        super(name, keySerde, valueSerde);
        this.validRecordPolicy = validRecordPolicy;
    }
}
