package org.galatea.kafka.starter.testing.conversion;

import java.util.Map;
import java.util.function.Function;

public class ConversionHelper {

  public static Object maybeConvert(String fieldPath, String fieldValue,
      Map<String, Function<String, Object>> conversionMap) {
    if (conversionMap.containsKey(fieldPath)) {
      return conversionMap.get(fieldPath).apply(fieldValue);
    }
    return fieldValue;
  }
}
