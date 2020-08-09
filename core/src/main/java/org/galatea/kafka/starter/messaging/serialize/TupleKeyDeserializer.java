package org.galatea.kafka.starter.messaging.serialize;

import com.apple.foundationdb.tuple.Tuple;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Deserializer;
import org.galatea.kafka.starter.messaging.serialize.exception.SerializationException;
import org.galatea.kafka.starter.messaging.serialize.util.StringConversion;

class TupleKeyDeserializer<T extends TupleKey> implements Deserializer<T> {

  private final Supplier<T> newInstanceCreator;
  private final List<BiConsumer<T, Object>> setFields;

  TupleKeyDeserializer(Supplier<T> newInstanceCreator, List<Field> sortedFields,
      Map<Class<?>, StringConversion<?>> additionalConverters) {
    this.newInstanceCreator = newInstanceCreator;

    setFields = sortedFields.stream().map(field -> (BiConsumer<T, Object>) (t, o) -> {
      try {
        StringConversion<?> conversion = additionalConverters.get(field.getType());
        Object converted = useConverterIfNecessary(o, conversion);
        field.set(t, converted);
      } catch (IllegalAccessException e) {
        throw new SerializationException("Could not Deserialize object of type " + t.getClass(),
            e);
      }
    }).collect(Collectors.toList());
  }

  private Object useConverterIfNecessary(Object fieldValue, StringConversion<?> conversion) {
    if (conversion != null && fieldValue != null) {
      return conversion.convertFromString().apply((String) fieldValue);
    }
    return fieldValue;
  }

  @Override
  public T deserialize(String s, byte[] bytes) {
    T deserialized = newInstanceCreator.get();

    List<Object> items = Tuple.fromBytes(bytes).getItems();
    for (int i = 0; i < setFields.size(); i++) {
      if (i >= items.size()) {
        // no more fields because null values were trimmed from the end to allow for range byte[] creation
        break;
      }
      Object obj = items.get(i);
      setFields.get(i).accept(deserialized, obj);
    }
    return deserialized;
  }
}
