package org.galatea.kafka.shell.domain;

import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
@RequiredArgsConstructor
public class SerdeType {

  public boolean isKey;
  public DataType type;

  public enum DataType {
    TUPLE, AVRO, LONG, INTEGER, SHORT, FLOAT, DOUBLE, STRING, BYTEBUFFER, BYTES, BYTE_ARRAY
  }
}
