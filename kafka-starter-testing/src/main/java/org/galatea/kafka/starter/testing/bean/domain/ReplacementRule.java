package org.galatea.kafka.starter.testing.bean.domain;

public interface ReplacementRule {
  ReplacementPredicate getPredicate();
  ReplacementSupplier getSupplier();

  static ReplacementRule of(ReplacementPredicate predicate, ReplacementSupplier supplier) {
    return new ReplacementRule() {
      @Override
      public ReplacementPredicate getPredicate() {
        return predicate;
      }

      @Override
      public ReplacementSupplier getSupplier() {
        return supplier;
      }
    };
  }
}
