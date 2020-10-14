package org.galatea.kafka.starter.testing.conversion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.galatea.kafka.starter.Pair;

public class ConversionUtil {

  private final Map<Class, List<Pair<Pattern, Function<String, ?>>>> typeConversionMap = new HashMap<>();

  public static Object convertFieldValue(String fieldPath, String fieldValue,
      Map<String, Function<String, Object>> conversionMap) {
    if (conversionMap.containsKey(fieldPath)) {
      return conversionMap.get(fieldPath).apply(fieldValue);
    }
    throw new IllegalArgumentException("Field conversion does not exist. Check whether conversion "
        + "exists before calling this method");
  }

  public static boolean hasFieldConversionMethod(String fieldPath,
      Map<String, Function<String, Object>> conversionMap) {
    return conversionMap.containsKey(fieldPath);
  }

  public <T> void registerTypeConversion(Class<T> forClass, Pattern matchPattern,
      Function<String, T> conversion) {
    List<Pair<Pattern, Function<String, T>>> conversionList = conversionsForType(forClass);
    conversionList.add(new Pair<>(matchPattern, conversion));
  }

  @SuppressWarnings("unchecked")
  private <T> List<Pair<Pattern, Function<String, T>>> conversionsForType(Class<T> forClass) {
    if (!typeConversionMap.containsKey(forClass)) {
      typeConversionMap.put(forClass, new ArrayList<>());
    }
    return (ArrayList) typeConversionMap.get(forClass);
  }

  /**
   * If there is are registered conversion that have a pattern that matches input, use registered
   * conversion. otherwise return input object unmodified.
   */
  public <T> Object maybeUseTypeConversion(Class<T> forType, String stringValue) {
    for (Pair<Pattern, Function<String, T>> conversionPair : conversionsForType(forType)) {
      if (conversionPair.getKey().matcher(stringValue).find()) {
        return conversionPair.getValue().apply(stringValue);
      }
    }
    return stringValue;
  }


}
