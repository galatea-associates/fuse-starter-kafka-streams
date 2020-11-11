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

  static <T> ReplacementRule ofTyped(Class<T> beanOfType, TypedReplacementSupplier<T> supplier) {
    ReplacementPredicate predicate = beanData -> beanOfType.isAssignableFrom(beanData.getBean().getClass());

    return new ReplacementRule() {
      @Override
      public ReplacementPredicate getPredicate() {
        return predicate;
      }

      @Override
      public TypedReplacementSupplier<T> getSupplier() {
        return supplier;
      }
    };
  }
}
