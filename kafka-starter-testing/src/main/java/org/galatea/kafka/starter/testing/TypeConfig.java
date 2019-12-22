package org.galatea.kafka.starter.testing;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TypeConfig<T> {
  private final Class<T> forClass;
  private final boolean isKeyType;
  private final Map<String, String> aliases = new HashMap<>();
  private final Map<String, Function<String, Object>> conversions = new HashMap<>();
}
