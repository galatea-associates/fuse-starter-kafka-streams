package org.galatea.kafka.starter.testing.avro.fieldtypes;

import org.galatea.kafka.starter.testing.avro.AvroFieldType;

public class AvroEnumType implements AvroFieldType {

  @Override
  public Object defaultValue(String... params) {
    return defaultValue(params[0], params[1]);
  }

  public Enum<?> defaultValue(String enumClassPath, String symbolName) {
    try {
      return Enum.valueOf(classFor(enumClassPath), symbolName);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          String.format("Could not find ENUM class %s", enumClassPath), e);
    }
  }

  private Class classFor(String classPath) throws ClassNotFoundException {
    return Class.forName(classPath);
  }
}
