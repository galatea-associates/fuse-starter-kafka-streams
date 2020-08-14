package org.galatea.kafka.starter.testing.avro;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.specific.SpecificRecord;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroArrayType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroBooleanType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroBytesType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroDoubleType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroEnumType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroFixedType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroFloatType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroIntType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroLocalDateType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroLocalTimeType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroLongType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroMapType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroStringType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroTimestampType;

/**
 * Process an avro record, making sure no fields have invalid values (like null values in nonnull
 * fields). Any invalid fields are updated to have valid values (nonnull fields are set to a default
 * value like empty string, 0, first enum symbol, etc).
 */
public class AvroPostProcessor<T extends SpecificRecord> implements RecordPostProcessor<T> {

  private static AvroPostProcessor<?> defaultUtil = null;

  public static <T extends SpecificRecord> AvroPostProcessor<T> defaultUtil() {
    if (defaultUtil == null) {
      defaultUtil = new AvroPostProcessor<>()
          .registerType(Type.ENUM, new AvroEnumType())
          .registerType(Type.ARRAY, new AvroArrayType())
          .registerType(Type.MAP, new AvroMapType())
          .registerType(Type.FIXED, new AvroFixedType())
          .registerType(Type.STRING, new AvroStringType())
          .registerType(Type.BYTES, new AvroBytesType())
          .registerType(Type.INT, new AvroIntType())
          .registerType(Type.LONG, new AvroLongType())
          .registerType(Type.FLOAT, new AvroFloatType())
          .registerType(Type.DOUBLE, new AvroDoubleType())
          .registerType(Type.BOOLEAN, new AvroBooleanType())
          .registerType(LogicalTypes.date(), new AvroLocalDateType())
          .registerType(LogicalTypes.timeMicros(), new AvroLocalTimeType())
          .registerType(LogicalTypes.timeMillis(), new AvroLocalTimeType())
          .registerType(LogicalTypes.timestampMicros(), new AvroTimestampType())
          .registerType(LogicalTypes.timestampMillis(), new AvroTimestampType());
    }
    return (AvroPostProcessor<T>) defaultUtil;
  }

  private final Map<Type, AvroFieldType> PRIMITIVE_TYPES = new HashMap<>();
  private final Map<LogicalType, AvroFieldType> LOGICAL_TYPES = new HashMap<>();

  public AvroPostProcessor<T> registerType(Type type, AvroFieldType fieldType) {
    PRIMITIVE_TYPES.put(type, fieldType);
    return this;
  }

  public AvroPostProcessor<T> registerType(LogicalType type, AvroFieldType fieldType) {
    LOGICAL_TYPES.put(type, fieldType);
    return this;
  }

  public T process(T message)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    if (message == null) {
      return null;
    }
    Schema schema = message.getSchema();

    for (Field field : schema.getFields()) {
      Object fieldValue = message.get(field.pos());
      if (fieldValue == null && !fieldNullable(field.schema())) {
        setFieldDefault(field, message);
      }
    }
    return message;
  }

  private void setFieldDefault(Field field, T message)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    LogicalType logicalType = fieldLogicalType(field.schema());
    Type primitiveType = fieldPrimitiveType(field.schema());

    if (field.hasDefaultValue()) {
      message.put(field.pos(), field.defaultVal());
      return;
    }

    AvroFieldType fieldType;
    if (logicalType != null) {
      fieldType = LOGICAL_TYPES.get(logicalType);
      if (fieldType == null) {
        throw new IllegalStateException(String.format("Logical type %s does not have a "
            + "registered default", logicalType));
      }
      message.put(field.pos(), fieldType.defaultValue());
    } else if (primitiveType.equals(Type.RECORD)) {
      T newRecord = (T) Class.forName(field.schema().getFullName()).newInstance();
      process(newRecord);
      message.put(field.pos(), newRecord);
    } else {
      fieldType = PRIMITIVE_TYPES.get(primitiveType);
      if (fieldType == null) {
        throw new IllegalStateException(String.format("Primitive type %s does not have a "
            + "registered default", primitiveType));
      }
      if (fieldType.getClass().equals(AvroEnumType.class)) {
        message.put(field.pos(), fieldType
            .defaultValue(field.schema().getFullName(), field.schema().getEnumSymbols().get(0)));
      } else {
        message.put(field.pos(), fieldType.defaultValue());
      }
    }
  }

  private LogicalType fieldLogicalType(Schema fieldSchema) {
    if (fieldSchema.getType().equals(Type.UNION)) {
      for (Schema unionType : fieldSchema.getTypes()) {
        if (!unionType.getType().equals(Type.NULL)) {
          return unionType.getLogicalType();
        }
      }
    }
    return fieldSchema.getLogicalType();
  }

  private Type fieldPrimitiveType(Schema fieldSchema) {
    if (fieldSchema.getType().equals(Type.UNION)) {
      for (Schema unionType : fieldSchema.getTypes()) {
        if (!unionType.getType().equals(Type.NULL)) {
          return unionType.getType();
        }
      }
    }
    return fieldSchema.getType();
  }

  private boolean fieldNullable(Schema fieldSchema) {
    switch (fieldSchema.getType()) {
      case UNION:
        for (Schema unionType : fieldSchema.getTypes()) {
          if (unionType.getType().equals(Type.NULL)) {
            return true;
          }
        }
        break;
      case NULL:
        return true;
    }
    return false;
  }


}
