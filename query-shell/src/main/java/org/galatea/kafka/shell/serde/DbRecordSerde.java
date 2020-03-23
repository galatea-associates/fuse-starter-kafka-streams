package org.galatea.kafka.shell.serde;

import com.apple.foundationdb.tuple.Tuple;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.galatea.kafka.shell.util.FieldExtractor;

@RequiredArgsConstructor
public class DbRecordSerde<T> implements Serde<T> {

  private final List<FieldExtractor<T>> properties;
  private final Callable<T> newObject;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public void close() {

  }

  @Override
  public Serializer<T> serializer() {
    return new Serializer<T>() {
      @Override
      public void configure(Map<String, ?> configs, boolean isKey) {

      }

      @Override
      public byte[] serialize(String topic, T data) {
        Object[] objects = properties.stream().map(prop -> prop.apply(data).getInner())
            .toArray();

        return Tuple.from(objects).pack();
      }

      @Override
      public void close() {

      }
    };
  }

  @Override
  public Deserializer<T> deserializer() {
    return new Deserializer<T>() {
      @Override
      public void configure(Map<String, ?> configs, boolean isKey) {

      }

      @Override
      public T deserialize(String topic, byte[] data) {
        Tuple deserialized = Tuple.fromBytes(data);
        T key;
        try {
          key = newObject.call();
        } catch (Exception e) {
          throw new IllegalArgumentException("Cannot construct new object to deserialize");
        }
        for (int i = 0; i < properties.size(); i++) {
          FieldExtractor<T> keyProperty = properties.get(i);
          Object o = deserialized.get(i);
          keyProperty.apply(key).setObject(o);
        }

        return key;
      }

      @Override
      public void close() {

      }
    };
  }
}
