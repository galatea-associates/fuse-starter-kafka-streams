package org.galatea.kafka.starter.messaging.streams;

import java.util.Collection;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Singular;

/**
 * Template for creating transformers.
 *
 * @param <K> Input key type
 * @param <V> Input value type
 * @param <K1> Output key type
 * @param <V1> Output value type
 * @param <T> Transformer-local state
 */
@Builder
@Getter
public class TransformerTemplate<K, V, K1, V1, T> {

    @Default
    private final StateInitializer<T> stateInitializer = () -> null;
    private final TransformMethod<K, V, K1, V1, T> transformMethod;
    @Default
    private final InitMethod<T> initMethod = (sp, state, context) -> {
    };
    @Default
    private final CloseMethod<T> closeMethod = (sp, state, context) -> {
    };

    @Singular
    private final Collection<TaskStoreRef<?, ?>> taskStores;

    @Singular
    private final Collection<TransformerPunctuate<K1, V1, T>> punctuates;

    @Default
    private final String name = null;
}
