package org.galatea.kafka.starter.messaging.streams.util;

import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GStreamConfigs {

  public static final Property<Duration> STORE_EXPIRATION_PUNCTUATE_INTERVAL = new Property<>(
      "store.expiration.punctuate.interval", Duration.ofHours(24));

  public static void checkValidityOfConfig(Map<String, Object> config) {
    Map<String, Class<?>> configurableKeysWithType = new HashMap<>();
    Arrays.stream(GStreamConfigs.class.getDeclaredFields())
        .filter(f -> Modifier.isStatic(f.getModifiers()))
        .filter(f -> f.getType() == Property.class)
        .forEach(field -> {
          try {
            Property<?> o = (Property<?>) field.get(null);
            configurableKeysWithType.put(o.getKey(), o.getDefaultValue().getClass());
          } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
          }
        });

    // check all props are the correct type
    config.forEach((key, value) -> {
      Class<?> expectedClass = configurableKeysWithType.get(key);
      if (expectedClass == null) {
        log.warn("Unknown GStreamConfig used: {}", key);
      } else if (!value.getClass().isAssignableFrom(expectedClass)) {
        String msg = String
            .format("Invalid GStreamConfig value type for %s; expected %s; actual: %s", key,
                expectedClass.getSimpleName(), value.getClass().getSimpleName());
        log.warn(msg);
        throw new IllegalStateException(msg);
      }
    });
  }

  @Value
  public static class Property<T> {

    String key;
    T defaultValue;
  }
}
