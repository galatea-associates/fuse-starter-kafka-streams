package org.galatea.kafka.starter.testing.bean.domain;

public interface BeanTypeReplacementRule<T> extends ReplacementRule {
  ReplacementPredicate getPredicate();
  ReplacementSupplier getSupplier();
}
