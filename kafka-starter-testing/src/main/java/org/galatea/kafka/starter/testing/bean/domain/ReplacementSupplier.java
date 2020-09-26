package org.galatea.kafka.starter.testing.bean.domain;

public interface ReplacementSupplier {

  Object get(SpringBeanData existing);
}
