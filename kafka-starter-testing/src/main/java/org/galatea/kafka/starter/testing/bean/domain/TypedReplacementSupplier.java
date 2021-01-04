package org.galatea.kafka.starter.testing.bean.domain;

public interface TypedReplacementSupplier<T> extends ReplacementSupplier {

  T get(SpringBeanData existing);
}
