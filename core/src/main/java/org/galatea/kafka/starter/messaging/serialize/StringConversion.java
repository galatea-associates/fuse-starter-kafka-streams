package org.galatea.kafka.starter.messaging.serialize;

import java.util.function.Function;

public interface StringConversion<T> {

  Function<T, String> convertToString();

  Function<String, T> convertFromString();

  static <U> StringConversion<U> from(Function<U, String> toString, Function<String, U> fromString) {
    return new StringConversion<U>() {
      @Override
      public Function<U, String> convertToString() {
        return toString;
      }

      @Override
      public Function<String, U> convertFromString() {
        return fromString;
      }
    };
  }
}
