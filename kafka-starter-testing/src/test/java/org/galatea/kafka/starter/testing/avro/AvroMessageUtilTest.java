package org.galatea.kafka.starter.testing.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.specific.SpecificRecord;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroEnumType;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class AvroMessageUtilTest {

  private AvroMessageUtil util;

  private static final Schema TEST_SCHEMA = new Schema.Parser().parse("{\"type\":\"record\","
      + "\"name\":\"TestMsg2\",\"namespace\":\"org.galatea.kafka.starter.messaging.test\",\"fields\":["
      + "{\"name\":\"doubleField\",\"type\":\"double\"},"
      + "{\"name\":\"dateField\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},"
      + "{\"name\":\"nullableField\",\"type\":[\"null\",\"double\"]},"
      + "{\"name\":\"defaultValField\",\"type\":\"double\",\"default\":1},"
      + "{\"name\":\"nonNullableEnum\",\"type\":{\"type\":\"enum\",\"name\":\"TestEnum\",\"symbols\":[\"ENUM_VALUE1\",\"ENUM_VALUE2\"]}},"
      + "{\"name\":\"subMessageField\",\"type\":{\"type\":\"record\",\"name\":\"TestSubMsg\",\"fields\":["
      + "{\"name\":\"nonNullableString\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},"
      + "{\"name\":\"nullableStringField\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}]},"
      + "{\"name\":\"nonNullableEnum\",\"type\":\"TestEnum\"}]}}]}");
  private SpecificRecord record;
  private Map<Integer, Object> recordValues;

  @Before
  public void setup() {
    util = new AvroMessageUtil();
    record = mock(SpecificRecord.class);
    recordValues = new HashMap<>();
    doReturn(TEST_SCHEMA).when(record).getSchema();
    doAnswer(invocationOnMock -> {
      recordValues.put(invocationOnMock.getArgument(0), invocationOnMock.getArgument(1));
      return null;
    }).when(record).put(anyInt(), any());
  }

  @Test
  @SneakyThrows
  public void useRegisteredPrimitiveType() {
    double defaultDoubleValue = 123D;
    LocalDate defaultLocalDateValue = LocalDate.ofEpochDay(1000);
    util.registerType(Type.DOUBLE, params -> defaultDoubleValue);
    util.registerType(LogicalTypes.date(), params -> defaultLocalDateValue);
    util.registerType(Type.STRING, params -> "");
    util.registerType(Type.ENUM, new AvroEnumType());

    // when
    util.populateRequiredFieldsWithDefaults(record);

    // then
    Field field = TEST_SCHEMA.getField("doubleField");
    verify(record, times(1)).put(field.pos(), defaultDoubleValue);
  }

  @Test
  @SneakyThrows
  public void useRegisteredLogicalType() {
    double defaultDoubleValue = 123D;
    LocalDate defaultLocalDateValue = LocalDate.ofEpochDay(1000);
    util.registerType(Type.DOUBLE, params -> defaultDoubleValue);
    util.registerType(LogicalTypes.date(), params -> defaultLocalDateValue);
    util.registerType(Type.STRING, params -> "");
    util.registerType(Type.ENUM, new AvroEnumType());

    // when
    util.populateRequiredFieldsWithDefaults(record);

    // then
    Field field = TEST_SCHEMA.getField("dateField");
    verify(record, times(1)).put(field.pos(), defaultLocalDateValue);
  }

  @Test
  @SneakyThrows
  public void useSchemaDefaultValue() {
    double defaultDoubleValue = 123D;
    LocalDate defaultLocalDateValue = LocalDate.ofEpochDay(1000);
    util.registerType(Type.DOUBLE, params -> defaultDoubleValue);
    util.registerType(LogicalTypes.date(), params -> defaultLocalDateValue);
    util.registerType(Type.STRING, params -> "");
    util.registerType(Type.ENUM, new AvroEnumType());

    // when
    util.populateRequiredFieldsWithDefaults(record);

    // then
    Field field = TEST_SCHEMA.getField("defaultValField");
    verify(record, times(1)).put(eq(field.pos()), eq(field.defaultVal()));
  }

  @Test
  @SneakyThrows
  public void nullableFieldIgnored() {
    double defaultDoubleValue = 123D;
    LocalDate defaultLocalDateValue = LocalDate.ofEpochDay(1000);
    util.registerType(Type.DOUBLE, params -> defaultDoubleValue);
    util.registerType(LogicalTypes.date(), params -> defaultLocalDateValue);
    util.registerType(Type.STRING, params -> "");
    util.registerType(Type.ENUM, new AvroEnumType());

    // when
    util.populateRequiredFieldsWithDefaults(record);

    // then
    Field field = TEST_SCHEMA.getField("nullableField");
    verify(record, never()).put(eq(field.pos()), any());
  }

  @Test(expected = IllegalStateException.class)
  @SneakyThrows
  public void noRegisteredType() {
    // given
    // do not register types

    // when
    util.populateRequiredFieldsWithDefaults(record);

    // should have thrown exception before this
    fail();
  }

  @Test
  @SneakyThrows
  public void enumValues() {
    double defaultDoubleValue = 123D;
    LocalDate defaultLocalDateValue = LocalDate.ofEpochDay(1000);
    util.registerType(Type.DOUBLE, params -> defaultDoubleValue);
    util.registerType(LogicalTypes.date(), params -> defaultLocalDateValue);
    util.registerType(Type.STRING, params -> "");
    util.registerType(Type.ENUM, new AvroEnumType());

    // when
    util.populateRequiredFieldsWithDefaults(record);

    Field field = TEST_SCHEMA.getField("nonNullableEnum");
    assertEquals(field.schema().getEnumSymbols().get(0), recordValues.get(field.pos()).toString());
  }
}