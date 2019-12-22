package org.galatea.kafka.starter.testing.avro.fieldtypes;

import java.util.HashMap;
import org.galatea.kafka.starter.testing.avro.AvroFieldType;

public class AvroMapType implements AvroFieldType {

  @Override
  public Object defaultValue(String... params) {
    return new HashMap<>();
  }
}
