package org.galatea.kafka.starter.messaging.serialize;

import static org.junit.Assert.assertEquals;

import org.apache.kafka.common.serialization.Serde;
import org.galatea.kafka.starter.messaging.serialize.domain.SimpleKey;
import org.junit.Before;
import org.junit.Test;

public class TupleKeySerdeTest {

  private Serde<SimpleKey> simpleSerde;

  @Before
  public void setup() {
    simpleSerde = new TupleKeySerde<>(SimpleKey.class);
  }

  @Test
  public void simpleKeyTest() {
    String value = "testThis";
    SimpleKey key = SimpleKey.builder().field1(value).build();

    byte[] serialized = simpleSerde.serializer().serialize(null, key);
    SimpleKey deserialized = simpleSerde.deserializer().deserialize(null, serialized);
    assertEquals(key, deserialized);
  }

  // TODO: single null value
  // null value at start
  // null value at tail
  // null value in middle
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