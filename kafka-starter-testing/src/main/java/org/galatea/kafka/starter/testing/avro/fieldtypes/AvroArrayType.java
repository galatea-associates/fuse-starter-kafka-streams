package org.galatea.kafka.starter.testing.avro.fieldtypes;

import java.util.ArrayList;
import org.galatea.kafka.starter.testing.avro.AvroFieldType;

public class AvroArrayType implements AvroFieldType {

  @Override
  public Object defaultValue(String... params) {
    return new ArrayList<>();
  }
}
