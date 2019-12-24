package org.galatea.kafka.starter.testing.bean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.galatea.kafka.starter.messaging.test.TestMsgKey;
import org.galatea.kafka.starter.messaging.test.TestMsgValue;
import org.galatea.kafka.starter.testing.TopicConfig;
import org.galatea.kafka.starter.testing.conversion.ConversionUtil;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class RecordBeanHelperTest {

  private ConversionUtil conversionUtil = new ConversionUtil();
  private TopicConfig<TestMsgKey, TestMsgValue> topicConfig;

  @Before
  public void setup() {
    Serde<TestMsgKey> mockKeySerde = mock(Serde.class);
    Serde<TestMsgValue> mockValueSerde = mock(Serde.class);

    topicConfig = new TopicConfig<>("testTopic", mockKeySerde, mockValueSerde, TestMsgKey::new,
        TestMsgValue::new);
  }

  @Test
  @SneakyThrows
  public void createRecord_keyAndValueCreated() {
    Map<String, String> fieldMap = new HashMap<>();
    fieldMap.put("keyValue", "123");

    KeyValue<TestMsgKey, TestMsgValue> record = RecordBeanHelper
        .createRecord(conversionUtil, fieldMap, topicConfig);

    assertEquals(TestMsgKey.class, record.key.getClass());
    assertEquals(TestMsgValue.class, record.value.getClass());
  }

  @Test
  @SneakyThrows
  public void createRecord_aliasSubstitution() {
    Map<String, String> fieldMap = new HashMap<>();
    String alias = "alias1";
    String fullyQualifiedField = "doubleField";
    topicConfig.getAliases().put(alias, fullyQualifiedField);

    String stringValue = "123";
    double expectedValue = 123;
    fieldMap.put(alias, stringValue);

    KeyValue<TestMsgKey, TestMsgValue> record = RecordBeanHelper
        .createRecord(conversionUtil, fieldMap, topicConfig);

    assertEquals(expectedValue, record.key.getDoubleField(), 0.0001);
    assertEquals(expectedValue, record.value.getDoubleField(), 0.0001);
  }

  @Test
  @SneakyThrows
  public void createRecord_fieldConversion() {
    Map<String, String> fieldMap = new HashMap<>();
    String fullyQualifiedField = "doubleField";

    topicConfig.getConversions()
        .put(fullyQualifiedField, stringValue -> Double.parseDouble(stringValue.replace("_", "")));

    String stringValue = "_123_";
    double expectedValue = 123;
    fieldMap.put(fullyQualifiedField, stringValue);

    KeyValue<TestMsgKey, TestMsgValue> record = RecordBeanHelper
        .createRecord(conversionUtil, fieldMap, topicConfig);

    assertEquals(expectedValue, record.key.getDoubleField(), 0.0001);
    assertEquals(expectedValue, record.value.getDoubleField(), 0.0001);
  }

  @Test
  @SneakyThrows
  public void createRecord_fieldConversionAliases() {
    Map<String, String> fieldMap = new HashMap<>();
    String alias = "alias1";
    String fullyQualifiedField = "doubleField";
    topicConfig.getAliases().put(alias, fullyQualifiedField);
    topicConfig.getConversions()
        .put(alias, stringValue -> Double.parseDouble(stringValue.replace("_", "")));

    String stringValue = "_123_";
    double expectedValue = 123;
    fieldMap.put(alias, stringValue);

    KeyValue<TestMsgKey, TestMsgValue> record = RecordBeanHelper
        .createRecord(conversionUtil, fieldMap, topicConfig);

    assertEquals(expectedValue, record.key.getDoubleField(), 0.0001);
    assertEquals(expectedValue, record.value.getDoubleField(), 0.0001);
  }

  @Test(expected = IllegalArgumentException.class)
  @SneakyThrows
  public void createRecord_allFieldsUsed() {
    Map<String, String> fieldMap = new HashMap<>();
    String fullyQualifiedField = "doubleField";
    String stringValue = "123";
    fieldMap.put(fullyQualifiedField, stringValue);
    fieldMap.put("nonExistentField", "someValue");

    RecordBeanHelper.createRecord(conversionUtil, fieldMap, topicConfig);

    fail();
  }

  @Test
  @SneakyThrows
  public void createRecord_keyPrefixExclusion() {
    Map<String, String> fieldMap = new HashMap<>();
    String fullyQualifiedField = "key.doubleField";

    String stringValue = "123";
    double expectedValue = 123;
    fieldMap.put(fullyQualifiedField, stringValue);

    KeyValue<TestMsgKey, TestMsgValue> record = RecordBeanHelper
        .createRecord(conversionUtil, fieldMap, topicConfig);

    assertEquals(expectedValue, record.key.getDoubleField(), 0.0001);
    assertNotEquals(expectedValue, record.value.getDoubleField(), 0.0001);
  }

  @Test
  @SneakyThrows
  public void createRecord_valuePrefixExclusion() {
    Map<String, String> fieldMap = new HashMap<>();
    String fullyQualifiedField = "value.doubleField";

    String stringValue = "123";
    double expectedValue = 123;
    fieldMap.put(fullyQualifiedField, stringValue);

    KeyValue<TestMsgKey, TestMsgValue> record = RecordBeanHelper
        .createRecord(conversionUtil, fieldMap, topicConfig);

    assertNotEquals(expectedValue, record.key.getDoubleField(), 0.0001);
    assertEquals(expectedValue, record.value.getDoubleField(), 0.0001);
  }

  @Test(expected = IllegalArgumentException.class)
  @SneakyThrows
  public void createRecord_unparseableStrings() {
    Map<String, String> fieldMap = new HashMap<>();
    String fullyQualifiedField = "doubleField";

    String stringValue = "_123_";
    fieldMap.put(fullyQualifiedField, stringValue);

    RecordBeanHelper.createRecord(conversionUtil, fieldMap, topicConfig);

    fail();
  }

  @Test
  @SneakyThrows
  public void copyRecordPropsIntoNew_aliasSubstitution() {
    Set<String> fieldsToCopy = new HashSet<>();

    KeyValue<TestMsgKey, TestMsgValue> originalRecord = RecordBeanHelper
        .createRecord(conversionUtil, Collections.emptyMap(), topicConfig);
    originalRecord.key.setDoubleField(123);
    originalRecord.value.setNonNullableStringField("string");

    String alias = "alias1";
    String fullyQualifiedField = "nonNullableStringField";
    topicConfig.getAliases().put(alias, fullyQualifiedField);
    fieldsToCopy.add(alias);

    // when
    KeyValue<TestMsgKey, TestMsgValue> outputRecord = RecordBeanHelper
        .copyRecordPropertiesIntoNew(fieldsToCopy, originalRecord, topicConfig);

    // then
    assertEquals(originalRecord.value.getNonNullableStringField(),
        outputRecord.value.getNonNullableStringField());
    assertNotEquals(originalRecord.key.getDoubleField(), outputRecord.key.getDoubleField());
  }

  @Test(expected = IllegalArgumentException.class)
  @SneakyThrows
  public void copyRecordPropsIntoNew_allFieldsUsed() {
    Set<String> fieldsToCopy = new HashSet<>();

    KeyValue<TestMsgKey, TestMsgValue> originalRecord = RecordBeanHelper
        .createRecord(conversionUtil, Collections.emptyMap(), topicConfig);
    originalRecord.key.setDoubleField(123);
    originalRecord.value.setNonNullableStringField("string");

    String fullyQualifiedField = "nonNullableStringField";
    fieldsToCopy.add(fullyQualifiedField);
    fieldsToCopy.add("nonExistentField");

    // when
    RecordBeanHelper.copyRecordPropertiesIntoNew(fieldsToCopy, originalRecord, topicConfig);

    fail();
  }

  @Test
  @SneakyThrows
  public void copyRecordPropsIntoNew_keyPrefixExclusion() {
    Set<String> fieldsToCopy = new HashSet<>();

    KeyValue<TestMsgKey, TestMsgValue> originalRecord = RecordBeanHelper
        .createRecord(conversionUtil, Collections.emptyMap(), topicConfig);
    originalRecord.key.setDoubleField(123);
    originalRecord.value.setDoubleField(234);

    String fullyQualifiedField = "key.doubleField";
    fieldsToCopy.add(fullyQualifiedField);

    // when
    KeyValue<TestMsgKey, TestMsgValue> outputRecord = RecordBeanHelper
        .copyRecordPropertiesIntoNew(fieldsToCopy, originalRecord, topicConfig);

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
        .createRecord(conversionUtil, Collections.emptyMap(), topicConfig);
    originalRecord.key.setDoubleField(123);
    originalRecord.value.setDoubleField(234);

    String fullyQualifiedField = "value.doubleField";
    fieldsToCopy.add(fullyQualifiedField);

    // when
    KeyValue<TestMsgKey, TestMsgValue> outputRecord = RecordBeanHelper
        .copyRecordPropertiesIntoNew(fieldsToCopy, originalRecord, topicConfig);

    // then
    assertNotEquals(originalRecord.key.getDoubleField(), outputRecord.key.getDoubleField(),
        0.00001);
    assertEquals(originalRecord.value.getDoubleField(), outputRecord.value.getDoubleField(),
        0.00001);
  }

  // TODO: tests around default values set in topicConfig
}