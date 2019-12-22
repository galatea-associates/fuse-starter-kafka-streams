package org.galatea.kafka.starter.testing.avro.fieldtypes;

import org.galatea.kafka.starter.testing.avro.AvroFieldType;

public class AvroFloatType implements AvroFieldType {

  @Override
  public Object defaultValue(String... params) {
    return 0F;
  }
}
