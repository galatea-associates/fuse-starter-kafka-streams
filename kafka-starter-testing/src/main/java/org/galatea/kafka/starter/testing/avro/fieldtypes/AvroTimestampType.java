package org.galatea.kafka.starter.testing.avro.fieldtypes;

import java.time.Instant;
import org.galatea.kafka.starter.testing.avro.AvroFieldType;

public class AvroTimestampType implements AvroFieldType {

  @Override
  public Object defaultValue(String... params) {
    return Instant.EPOCH;
  }
}
