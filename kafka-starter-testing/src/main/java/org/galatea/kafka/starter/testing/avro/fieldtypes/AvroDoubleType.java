package org.galatea.kafka.starter.testing.avro.fieldtypes;

import org.galatea.kafka.starter.testing.avro.AvroFieldType;

public class AvroDoubleType implements AvroFieldType {

  @Override
  public Object defaultValue(String... params) {
    return 0D;
  }
}
