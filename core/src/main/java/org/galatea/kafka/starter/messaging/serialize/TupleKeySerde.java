package org.galatea.kafka.starter.messaging.serialize;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.galatea.kafka.starter.messaging.serialize.exception.SerializationException;

@Slf4j
public class TupleKeySerde<T extends TupleKey> implements Serde<T> {

  private final TupleKeySerializer<T> serializer;
  private final TupleKeyDeserializer<T> deserializer;

  public TupleKeySerde(Class<T> forClass) {

    Supplier<T> newInstanceCreator = newInstanceCreator(forClass);

    // Test to see if newInstanceCreator will work
    try {
      newInstanceCreator.get();
    } catch (SerializationException e) {
      throw new RuntimeException(
          "Cannot create Serde due to inaccessible no-arg constructor in " + forClass.getName(), e);
    }

    // verify no fields have same ordinal, and put fields in map
    List<Field> sortedFields = getSortedTupleFields(forClass);
    if (sortedFields.isEmpty()) {
      throw new RuntimeException(String
          .format("Class %s has no fields annotated with %s", forClass.getSimpleName(),
              TupleField.class.getSimpleName()));
    }
    sortedFields.forEach(f -> f.setAccessible(true));

    serializer = new TupleKeySerializer<>(sortedFields);
    deserializer = new TupleKeyDeserializer<>(newInstanceCreator, sortedFields);
  }

  /**
   * sort fields based on {@link TupleField} value
   */
  private List<Field> getSortedTupleFields(Class<T> forClass) {
    Map<Integer, Field> fieldOrderMap = new HashMap<>();
    for (Field field : forClass.getDeclaredFields()) {
      if (field.isAnnotationPresent(TupleField.class)) {
        int order = field.getAnnotation(TupleField.class).value();

        if (fieldOrderMap.put(order, field) != null) {
          throw new RuntimeException(
              "Multiple fields have the same ordinal in " + forClass.getName());
        }
      }
    }

    // sort fields by ordinal
    return fieldOrderMap.entrySet().stream().sorted(Entry.comparingByKey())
        .map(Entry::getValue).collect(Collectors.toList());
  }

  private Supplier<T> newInstanceCreator(Class<T> forClass) {
    return () -> {
      try {
        return forClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new SerializationException("Cannot instantiate new object for deserialization", e);
      }
    };
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public void close() {

  }

  @Override
  public Serializer<T> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<T> deserializer() {
    return deserializer;
  }
}
