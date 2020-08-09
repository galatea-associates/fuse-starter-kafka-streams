package org.galatea.kafka.starter.messaging.serialize;

import static org.junit.Assert.assertEquals;

import org.apache.kafka.common.serialization.Serde;
import org.galatea.kafka.starter.messaging.serialize.domain.DualValueKey;
import org.galatea.kafka.starter.messaging.serialize.domain.SingleValueKey;
import org.galatea.kafka.starter.messaging.serialize.domain.TripleValueKey;
import org.junit.Before;
import org.junit.Test;

public class TupleKeySerdeTest {

  private Serde<SingleValueKey> singleValueSerde;
  private Serde<DualValueKey> dualValueSerde;
  private Serde<TripleValueKey> tripleValueSerde;

  @Before
  public void setup() {
    singleValueSerde = new TupleKeySerde<>(SingleValueKey.class);
    dualValueSerde = new TupleKeySerde<>(DualValueKey.class);
    tripleValueSerde = new TupleKeySerde<>(TripleValueKey.class);
  }

  @Test
  public void singleValue() {
    SingleValueKey key = SingleValueKey.builder().field1("testValue").build();

    assertEquals(key, serializeCycle(key));
  }

  @Test
  public void singleNullValue() {
    SingleValueKey key = SingleValueKey.builder().field1(null).build();

    assertEquals(key, serializeCycle(key));
  }

  @Test
  public void nullValueInFirstPosition() {
    DualValueKey key = DualValueKey.builder().field2("test").build();

    assertEquals(key, serializeCycle(key));
  }

  @Test
  public void nullValueInLastPosition() {
    DualValueKey key = DualValueKey.builder().field1("test").build();

    assertEquals(key, serializeCycle(key));
  }

  @Test
  public void nullValueInMiddlePosition() {
    TripleValueKey key = TripleValueKey.builder().field3("test").field1("test2").build();

    assertEquals(key, serializeCycle(key));
  }

  private SingleValueKey serializeCycle(SingleValueKey key) {
    byte[] serialized = singleValueSerde.serializer().serialize(null, key);
    return singleValueSerde.deserializer().deserialize(null, serialized);
  }

  private DualValueKey serializeCycle(DualValueKey key) {
    byte[] serialized = dualValueSerde.serializer().serialize(null, key);
    return dualValueSerde.deserializer().deserialize(null, serialized);
  }

  private TripleValueKey serializeCycle(TripleValueKey key) {
    byte[] serialized = tripleValueSerde.serializer().serialize(null, key);
    return tripleValueSerde.deserializer().deserialize(null, serialized);
  }

  // range: all values
  // range: null value at start
  // range: null value at end
  // range: single null value
  // conversion: null value at end
  // conversion: null value in mid
  // conversion: null value at start
  // error: no-arg constructor missing
  // error: no annotated fields
  // error: instantiation error not on startup, when doing real deser (mock class, make it throw exception on 2nd newInstance)

}