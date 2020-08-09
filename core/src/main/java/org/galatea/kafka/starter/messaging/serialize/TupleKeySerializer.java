package org.galatea.kafka.starter.messaging.serialize;

import com.apple.foundationdb.tuple.Tuple;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serializer;
import org.galatea.kafka.starter.messaging.serialize.exception.SerializationException;
import org.galatea.kafka.starter.messaging.serialize.util.StringConversion;

class TupleKeySerializer<T extends TupleKey> implements Serializer<T> {

  private final List<Function<T, Object>> extractFields;

  TupleKeySerializer(List<Field> sortedFields,
      Map<Class<?>, StringConversion<?>> additionalConverters) {
    extractFields = sortedFields.stream().map(field -> (Function<T, Object>) t -> {
      try {
        Object extractedValue = field.get(t);
        StringConversion conversion = additionalConverters.get(field.getType());
        return convertIfNecessary(extractedValue, conversion);
      } catch (IllegalAccessException e) {
        throw new SerializationException(
            "Could not get property from bean; Field:" + field + "; Bean:" + t, e);
      }
    }).collect(Collectors.toList());
  }

  private <U> Object convertIfNecessary(U extractedValue, StringConversion<U> conversion) {
    if (conversion != null && extractedValue != null) {
      return conversion.convertToString().apply(extractedValue);
    }
    return extractedValue;
  }

  @Override
  public byte[] serialize(String s, T t) {
    List<Object> objects = extractFields.stream().map(f -> f.apply(t)).collect(Collectors.toList());

    // remove trailing null values. This allows creation of TupleKey objects that can act as
    // range begin and end
    for (int i = objects.size() - 1; i >= 0; i--) {
      if (objects.get(i) == null) {
        objects.remove(i);
      } else {
        break;
      }
    }
    return Tuple.fromList(objects).pack();
  }
}
