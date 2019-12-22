package org.galatea.kafka.starter.testing.avro.fieldtypes;

import java.time.LocalTime;
import org.galatea.kafka.starter.testing.avro.AvroFieldType;

public class AvroLocalTimeType implements AvroFieldType {

  @Override
  public Object defaultValue(String... params) {
    return LocalTime.MIDNIGHT;
  }
}
