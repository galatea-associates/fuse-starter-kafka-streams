package org.galatea.kafka.starter.messaging.streams;

import java.util.Collection;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Singular;

/**
 * Template for creating processors.
 *
 * @param <K> Input key type
 * @param <V> Input value type
 * @param <T> Transformer-local state
 */
@Builder
@Getter
public class ProcessorTemplate<K, V, T> {

  @Default
  private final StateInitializer<T> stateInitializer = () -> null;
  private final ProcessMethod<K, V, T> transformMethod;
  @Default
  private final InitMethod<T> initMethod = (sp, state, context) -> {
  };
  @Default
  private final CloseMethod<T> closeMethod = (sp, state, context) -> {
  };

  @Singular
  private final Collection<TaskStoreRef<?, ?>> taskStores;

  @Singular
  private final Collection<ProcessorPunctuate<T>> punctuates;

  @Default
  private final String name = null;
}
