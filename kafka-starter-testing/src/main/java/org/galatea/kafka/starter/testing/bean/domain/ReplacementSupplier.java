package org.galatea.kafka.starter.testing.bean.domain;

public interface ReplacementSupplier<T> {

  T get(SpringBeanData<T> existing);
}
