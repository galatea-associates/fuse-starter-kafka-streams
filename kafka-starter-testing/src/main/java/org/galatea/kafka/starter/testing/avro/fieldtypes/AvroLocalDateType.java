package org.galatea.kafka.starter.testing.avro.fieldtypes;

import java.time.LocalDate;
import org.galatea.kafka.starter.testing.avro.AvroFieldType;

public class AvroLocalDateType implements AvroFieldType {

  @Override
  public Object defaultValue(String... params) {
    return LocalDate.ofEpochDay(0);
  }
}
