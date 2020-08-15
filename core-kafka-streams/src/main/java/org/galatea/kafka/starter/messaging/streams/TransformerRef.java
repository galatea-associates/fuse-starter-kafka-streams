package org.galatea.kafka.starter.messaging.streams;

import lombok.AccessLevel;
import lombok.Getter;
import org.apache.kafka.streams.KeyValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;

public abstract class TransformerRef<K, V, K1, V1, T> implements TaskStoreSupplier {

  @Autowired(required = false)
  @Getter(AccessLevel.PACKAGE)
  private ConfigurableBeanFactory beanFactory;

  public T initState() {
    return null;
  }

  public void init(ProcessorTaskContext<K1, V1, T> context) {
    // do nothing
  }

  abstract public KeyValue<K1, V1> transform(K key, V value,
      ProcessorTaskContext<K1, V1, T> context);

  public void close(ProcessorTaskContext<K1, V1, T> context) {
    // do nothing
  }

//  Collection<ProcessorPunctuate<K1, V1, T>> punctuates() {
//    return Collections.emptyList();
//  }
}
