package org.galatea.kafka.starter.testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.galatea.kafka.starter.messaging.test.TestMsgKey;
import org.galatea.kafka.starter.messaging.test.TestMsgValue;
import org.galatea.kafka.starter.testing.conversion.ConversionUtil;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class RecordBeanHelperTest {

  private final ConversionUtil conversionUtil = new ConversionUtil();
  private TopicConfig<TestMsgKey, TestMsgValue> beanTopicConfig;
  private TopicConfig<String, String> primitiveTopicConfig;

  @Before
  public void setup() {
    Serde<TestMsgKey> mockKeySerde = mock(Serde.class);
    Serde<TestMsgValue> mockValueSerde = mock(Serde.class);

    beanTopicConfig = new TopicConfig<>("testTopic", mockKeySerde, mockValueSerde, TestMsgKey::new,
        TestMsgValue::new);
    primitiveTopicConfig = new TopicConfig<>("testTopic2", Serdes.String(), Serdes.String(),
        String::new, String::new);
  }

  @Test
  @SneakyThrows
  public void createBeanRecord_keyAndValueCreated() {
    Map<String, String> fieldMap = new HashMap<>();
    fieldMap.put("keyValue", "123");

    KeyValue<TestMsgKey, TestMsgValue> record = RecordBeanHelper
        .createRecord(conversionUtil, fieldMap, beanTopicConfig, true, true);

    assertEquals(TestMsgKey.class, record.key.getClass());
    assertEquals(TestMsgValue.class, record.value.getClass());
  }

  @Test
  @SneakyThrows
  public void createPrimitiveRecord_keyAndValueCreated() {
    Map<String, String> fieldMap = new HashMap<>();
    fieldMap.put("KEY", "123");
    fieldMap.put("VALUE", "1234");

    KeyValue<String, String> record = RecordBeanHelper
        .createRecord(conversionUtil, fieldMap, primitiveTopicConfig, false, false);

    assertEquals(String.class, record.key.getClass());
    assertEquals(String.class, record.value.getClass());
  }

  @Test
  @SneakyThrows
  public void createBeanRecord_aliasSubstitution() {
    Map<String, String> fieldMap = new HashMap<>();
    String alias = "alias1";
    String fullyQualifiedField = "doubleField";
    beanTopicConfig.registerAlias(alias, fullyQualifiedField);

    String stringValue = "123";
    double expectedValue = 123;
    fieldMap.put(alias, stringValue);

    KeyValue<TestMsgKey, TestMsgValue> record = RecordBeanHelper
        .createRecord(conversionUtil, fieldMap, beanTopicConfig, true, true);

    assertEquals(expectedValue, record.key.getDoubleField(), 0.0001);
    assertEquals(expectedValue, record.value.getDoubleField(), 0.0001);
  }

  @Test
  @SneakyThrows
  public void createBeanRecord_fieldConversion() {
    Map<String, String> fieldMap = new HashMap<>();
    String fullyQualifiedField = "doubleField";

    beanTopicConfig.registerConversion(fullyQualifiedField,
        stringValue -> Double.parseDouble(stringValue.replace("_", "")));

    String stringValue = "_123_";
    double expectedValue = 123;
    fieldMap.put(fullyQualifiedField, stringValue);

    KeyValue<TestMsgKey, TestMsgValue> record = RecordBeanHelper
        .createRecord(conversionUtil, fieldMap, beanTopicConfig, true, true);

    assertEquals(expectedValue, record.key.getDoubleField(), 0.0001);
    assertEquals(expectedValue, record.value.getDoubleField(), 0.0001);
  }

  @Test
  @SneakyThrows
  public void createBeanRecord_fieldConversionAliases() {
    Map<String, String> fieldMap = new HashMap<>();
    String alias = "alias1";
    String fullyQualifiedField = "doubleField";
    beanTopicConfig.registerAlias(alias, fullyQualifiedField);
    beanTopicConfig
        .registerConversion(alias, stringValue -> Double.parseDouble(stringValue.replace("_", "")));

    String stringValue = "_123_";
    double expectedValue = 123;
    fieldMap.put(alias, stringValue);

    KeyValue<TestMsgKey, TestMsgValue> record = RecordBeanHelper
        .createRecord(conversionUtil, fieldMap, beanTopicConfig, true, true);

    assertEquals(expectedValue, record.key.getDoubleField(), 0.0001);
    assertEquals(expectedValue, record.value.getDoubleField(), 0.0001);
  }

  @Test(expected = IllegalArgumentException.class)
  @SneakyThrows
  public void createBeanRecord_allFieldsUsed() {
    Map<String, String> fieldMap = new HashMap<>();
    String fullyQualifiedField = "doubleField";
    String stringValue = "123";
    fieldMap.put(fullyQualifiedField, stringValue);
    fieldMap.put("nonExistentField", "someValue");

    RecordBeanHelper.createRecord(conversionUtil, fieldMap, beanTopicConfig, true, true);

    fail();
  }

  @Test(expected = IllegalArgumentException.class)
  @SneakyThrows
  public void createPrimitiveRecord_allFieldsUsed() {
    Map<String, String> fieldMap = new HashMap<>();
    fieldMap.put("KEY", "123");
    fieldMap.put("VALUE", "1234");
    fieldMap.put("nonExistentField", "someValue");

    RecordBeanHelper.createRecord(conversionUtil, fieldMap, primitiveTopicConfig, false, false);

    fail();
  }

  @Test
  @SneakyThrows
  public void createBeanRecord_keyPrefixExclusion() {
    Map<String, String> fieldMap = new HashMap<>();
    String fullyQualifiedField = "key.doubleField";

    String stringValue = "123";
    double expectedValue = 123;
    fieldMap.put(fullyQualifiedField, stringValue);

    KeyValue<TestMsgKey, TestMsgValue> record = RecordBeanHelper
        .createRecord(conversionUtil, fieldMap, beanTopicConfig, true, true);

    assertEquals(expectedValue, record.key.getDoubleField(), 0.0001);
    assertNotEquals(expectedValue, record.value.getDoubleField(), 0.0001);
  }

  @Test
  @SneakyThrows
  public void createBeanRecord_valuePrefixExclusion() {
    Map<String, String> fieldMap = new HashMap<>();
    String fullyQualifiedField = "value.doubleField";

    String stringValue = "123";
    double expectedValue = 123;
    fieldMap.put(fullyQualifiedField, stringValue);

    KeyValue<TestMsgKey, TestMsgValue> record = RecordBeanHelper
        .createRecord(conversionUtil, fieldMap, beanTopicConfig, true, true);

    assertNotEquals(expectedValue, record.key.getDoubleField(), 0.0001);
    assertEquals(expectedValue, record.value.getDoubleField(), 0.0001);
  }

  @Test(expected = IllegalArgumentException.class)
  @SneakyThrows
  public void createBeanRecord_unparseableStrings() {
    Map<String, String> fieldMap = new HashMap<>();
    String fullyQualifiedField = "doubleField";

    String stringValue = "_123_";
    fieldMap.put(fullyQualifiedField, stringValue);

    RecordBeanHelper.createRecord(conversionUtil, fieldMap, beanTopicConfig, true, true);

    fail();
  }

  @Test
  @SneakyThrows
  public void copyRecordPropsIntoNew_aliasSubstitution() {
    Set<String> fieldsToCopy = new HashSet<>();

    KeyValue<TestMsgKey, TestMsgValue> originalRecord = RecordBeanHelper
        .createRecord(conversionUtil, Collections.emptyMap(), beanTopicConfig, true, true);
    originalRecord.key.setDoubleField(123);
    originalRecord.value.setNonNullableStringField("string");

    String alias = "alias1";
    String fullyQualifiedField = "nonNullableStringField";
    beanTopicConfig.registerAlias(alias, fullyQualifiedField);
    fieldsToCopy.add(alias);

    // when
    KeyValue<TestMsgKey, TestMsgValue> outputRecord = RecordBeanHelper
        .copyRecordPropertiesIntoNew(fieldsToCopy, originalRecord, beanTopicConfig, true, true);

    // then
    assertEquals(originalRecord.value.getNonNullableStringField(),
        outputRecord.value.getNonNullableStringField());
    assertNotEquals(originalRecord.key.getDoubleField(), outputRecord.key.getDoubleField());
  }

  @Test(expected = IllegalArgumentException.class)
  @SneakyThrows
  public void copyBeanRecordPropsIntoNew_allFieldsUsed() {
    Set<String> fieldsToCopy = new HashSet<>();

    KeyValue<TestMsgKey, TestMsgValue> originalRecord = RecordBeanHelper
        .createRecord(conversionUtil, Collections.emptyMap(), beanTopicConfig, true, true);
    originalRecord.key.setDoubleField(123);
    originalRecord.value.setNonNullableStringField("string");

    String fullyQualifiedField = "nonNullableStringField";
    fieldsToCopy.add(fullyQualifiedField);
    fieldsToCopy.add("nonExistentField");

    // when
    RecordBeanHelper.copyRecordPropertiesIntoNew(fieldsToCopy, originalRecord, beanTopicConfig,
        true, true);

    fail();
  }

  @Test(expected = IllegalArgumentException.class)
  @SneakyThrows
  public void copyPrimitiveRecordPropsIntoNew_allFieldsUsed() {
    Set<String> fieldsToCopy = new HashSet<>();

    KeyValue<String, String> originalRecord = KeyValue.pair("key", "value");
    fieldsToCopy.add("KEY");
    fieldsToCopy.add("VALUE");
    fieldsToCopy.add("nonExistentField");

    // when
    RecordBeanHelper.copyRecordPropertiesIntoNew(fieldsToCopy, originalRecord, primitiveTopicConfig,
        false, false);

    fail();
  }

  @Test
  @SneakyThrows
  public void copyRecordPropsIntoNew_keyPrefixExclusion() {
    Set<String> fieldsToCopy = new HashSet<>();

    KeyValue<TestMsgKey, TestMsgValue> originalRecord = RecordBeanHelper
        .createRecord(conversionUtil, Collections.emptyMap(), beanTopicConfig, true, true);
    originalRecord.key.setDoubleField(123);
    originalRecord.value.setDoubleField(234);

    String fullyQualifiedField = "key.doubleField";
    fieldsToCopy.add(fullyQualifiedField);

    // when
    KeyValue<TestMsgKey, TestMsgValue> outputRecord = RecordBeanHelper
        .copyRecordPropertiesIntoNew(fieldsToCopy, originalRecord, beanTopicConfig, true, true);

    // then
    assertEquals(originalRecord.key.getDoubleField(), outputRecord.key.getDoubleField(), 0.00001);
    assertNotEquals(originalRecord.value.getDoubleField(), outputRecord.value.getDoubleField(),
        0.00001);
  }

  @Test
  @SneakyThrows
  public void copyRecordPropsIntoNew_valuePrefixExclusion() {
    Set<String> fieldsToCopy = new HashSet<>();

    KeyValue<TestMsgKey, TestMsgValue> originalRecord = RecordBeanHelper
        .createRecord(conversionUtil, Collections.emptyMap(), beanTopicConfig, true, true);
    originalRecord.key.setDoubleField(123);
    originalRecord.value.setDoubleField(234);

    String fullyQualifiedField = "value.doubleField";
    fieldsToCopy.add(fullyQualifiedField);

    // when
    KeyValue<TestMsgKey, TestMsgValue> outputRecord = RecordBeanHelper
        .copyRecordPropertiesIntoNew(fieldsToCopy, originalRecord, beanTopicConfig, true, true);

    // then
    assertNotEquals(originalRecord.key.getDoubleField(), outputRecord.key.getDoubleField(),
        0.00001);
    assertEquals(originalRecord.value.getDoubleField(), outputRecord.value.getDoubleField(),
        0.00001);
  }

  @Test
  @SneakyThrows
  public void createBeanRecord_nullableFieldDefaultNull() {
    Map<String, String> fieldMap = new HashMap<>();

    KeyValue<TestMsgKey, TestMsgValue> record = RecordBeanHelper
        .createRecord(conversionUtil, fieldMap, beanTopicConfig, true, true);

    assertNull(record.value.getNullableStringField());
  }

  @Test
  @SneakyThrows
  public void createBeanRecord_definedFieldDefault() {
    Map<String, String> fieldMap = new HashMap<>();
    String defaultValue = "defValue";
    beanTopicConfig.registerDefault("nullableStringField", defaultValue);
    beanTopicConfig.registerDefault("nonNullableStringField", defaultValue);

    KeyValue<TestMsgKey, TestMsgValue> record = RecordBeanHelper
        .createRecord(conversionUtil, fieldMap, beanTopicConfig, true, true);

    assertEquals(defaultValue, record.value.getNullableStringField());
    assertEquals(defaultValue, record.value.getNonNullableStringField());
  }

  @Test
  @SneakyThrows
  public void createBeanRecord_definedFieldDefaultConvertedUsingConversion() {
    Map<String, String> fieldMap = new HashMap<>();
    String defaultValue = "defValue";
    String fieldName = "nonNullableStringField";
    Function<String, Object> conversionFunction = stringValue -> stringValue.toLowerCase()
        + stringValue.toUpperCase();

    beanTopicConfig.registerDefault(fieldName, defaultValue);
    beanTopicConfig.registerConversion(fieldName, conversionFunction);

    KeyValue<TestMsgKey, TestMsgValue> record = RecordBeanHelper
        .createRecord(conversionUtil, fieldMap, beanTopicConfig, true, true);

    assertEquals(conversionFunction.apply(defaultValue), record.value.getNonNullableStringField());
  }

  @Test
  @SneakyThrows
  public void createBeanRecord_aliasDefinedFieldDefault() {
    Map<String, String> fieldMap = new HashMap<>();
    String defaultValue = "defValue";
    String fieldAlias = "fieldAlias";
    String fieldName = "nonNullableStringField";

    beanTopicConfig.registerDefault(fieldAlias, defaultValue);
    beanTopicConfig.registerAlias(fieldAlias, fieldName);

    KeyValue<TestMsgKey, TestMsgValue> record = RecordBeanHelper
        .createRecord(conversionUtil, fieldMap, beanTopicConfig, true, true);

    assertEquals(defaultValue, record.value.getNonNullableStringField());
  }

  @Test
  @SneakyThrows
  public void createBeanRecord_setFieldNull() {
    Map<String, String> fieldMap = new HashMap<>();
    fieldMap.put("nullableStringField", "<null>");

    KeyValue<TestMsgKey, TestMsgValue> record = RecordBeanHelper
        .createRecord(conversionUtil, fieldMap, beanTopicConfig, true, true);

    assertNull(record.value.getNullableStringField());
  }

}