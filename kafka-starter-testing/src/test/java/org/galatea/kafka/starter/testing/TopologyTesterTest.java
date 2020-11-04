package org.galatea.kafka.starter.testing;

import static org.junit.Assert.assertEquals;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.test.TestMsgKey;
import org.galatea.kafka.starter.messaging.test.TestMsgValue;
import org.galatea.kafka.starter.messaging.test.TestSubMsg;
import org.galatea.kafka.starter.testing.conversion.ConversionService;
import org.galatea.kafka.starter.testing.avro.AvroPostProcessor;
import org.galatea.kafka.starter.testing.conversion.ConversionUtil;
import org.junit.Before;
import org.junit.Test;

public class TopologyTesterTest {

  private static TopologyTester tester;
  private static final Topology testTopology = TestTopology.topology();
  private static final Topic<String, String> inputTopic1 = TestTopology.inputTopic;
  private static final Topic<TestMsgKey, TestMsgValue> inputTopic2 = TestTopology.inputTopic2;
  private static final Topic<TestMsgKey, TestSubMsg> inputTopic3 = TestTopology.inputTopic3;
  private static final Topic<TestMsgKey, TestMsgValue> inputTopic4 = TestTopology.inputTopic4;

  private static final Topic<String, String> outputTopic1 = TestTopology.outputTopic;
  private static final Topic<TestMsgKey, TestMsgValue> outputTopic2 = TestTopology.outputTopic2;
  private static final Topic<TestMsgKey, TestSubMsg> outputTopic3 = TestTopology.outputTopic3;
  private static final Topic<TestMsgKey, TestMsgValue> outputTopic4 = TestTopology.outputTopic4;
  private static final String storeName1 = TestTopology.STORE_NAME1;
  private static final String storeName2 = TestTopology.STORE_NAME2;
  private static final String storeName3 = TestTopology.STORE_NAME3;
  private static final String storeName4 = TestTopology.STORE_NAME4;
  private static final Properties streamProperties;
  private static final AtomicLong lastTesterId = new AtomicLong(0);
  public static final LocalDate REF_DATE = LocalDate.of(2020, 1, 1);

  static {
    streamProperties = new Properties();
    streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app.id");
    streamProperties.put(StreamsConfig.STATE_DIR_CONFIG, "target/testStateDir");
    streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:65535");
  }

  private TopologyTester newTester() {
    TopologyTester tester = newTester(streamProperties(lastTesterId.incrementAndGet()));
    tester.registerPostProcessor(SpecificRecord.class, AvroPostProcessor.defaultUtil());
    return tester;
  }

  private TopologyTester newTesterWithoutAvroRegistered() {
    return newTester(streamProperties(lastTesterId.incrementAndGet()));
  }

  private TopologyTester newTester(Properties props) {
    TopologyTester tester = new TopologyTester(testTopology, props);

    tester.configureInputTopic(inputTopic1, String::new, String::new);
    tester.configureInputTopic(inputTopic2, TestMsgKey::new, TestMsgValue::new);
    tester.configureInputTopic(inputTopic3, TestMsgKey::new, TestSubMsg::new);
    tester.configureInputTopic(inputTopic4, TestMsgKey::new, TestMsgValue::new);
    tester.configureOutputTopic(outputTopic1, String::new, String::new);
    tester.configureOutputTopic(outputTopic2, TestMsgKey::new, TestMsgValue::new);
    tester.configureOutputTopic(outputTopic3, TestMsgKey::new, TestSubMsg::new);
    tester.configureOutputTopic(outputTopic4, TestMsgKey::new, TestMsgValue::new);

    tester.configureStore(storeName1, Serdes.String(), Serdes.String(), String::new, String::new);
    tester.configureStore(storeName2, inputTopic2.getKeySerde(), inputTopic2.getValueSerde(),
        TestMsgKey::new, TestMsgValue::new);
    tester.configureStore(storeName3, inputTopic3.getKeySerde(), inputTopic3.getValueSerde(),
        TestMsgKey::new, TestSubMsg::new);
    tester.configureStore(storeName4, inputTopic4.getKeySerde(), inputTopic4.getValueSerde(),
        TestMsgKey::new, TestMsgValue::new);

    tester.registerBeanClass(TestMsgKey.class);
    tester.registerBeanClass(TestMsgValue.class);

    Pattern relativeTDatePattern = Pattern.compile("^\\s*[Tt]\\s*(([+-])\\s*(\\d+)\\s*)?$");
    ConversionService typeConversionService = tester.getTypeConversionService();
    typeConversionService.registerTypeConversion(LocalDate.class, Pattern.compile("^\\d+$"),
        (stringValue, matcher) -> LocalDate.ofEpochDay(Long.parseLong(stringValue)));
    typeConversionService.registerTypeConversion(LocalDate.class, relativeTDatePattern,
        (tDateString, matcher) -> {
          if (matcher.group(1) != null) {
            String plusMinus = matcher.group(2);
            long numDays = Long.parseLong(matcher.group(3));
            if (plusMinus.equals("+")) {
              return REF_DATE.plusDays(numDays);
            } else if (plusMinus.equals("-")) {
              return REF_DATE.minusDays(numDays);
            } else {
              throw new IllegalArgumentException(
                  "Group 2 of regex expected to be either '+' or '-'");
            }
          }
          return REF_DATE;
        });

    return tester;
  }

  private Properties streamProperties(long testerId) {
    Properties newProps = new Properties();
    newProps.putAll(streamProperties);
    newProps.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "app.id" + testerId);
    return newProps;
  }

  @Before
  public void setup() {
    if (tester == null) {
      tester = newTester();
    }

    tester.beforeTest();
  }

  @Test
  @SneakyThrows
  public void inputOutputProduced() {
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("KEY", "key");
    inputRecord.put("VALUE", "value");

    List<Map<String, String>> expected = new ArrayList<>();
    Map<String, String> record1 = new HashMap<>();
    record1.put("KEY", "key");
    record1.put("VALUE", "VALUE");
    expected.add(record1);

    // when
    tester.pipeInput(inputTopic1, inputRecord);

    // then
    tester.assertOutputList(outputTopic1, expected, false);
  }

  @Test
  @SneakyThrows
  public void mockStreamsRetrieveStore() {
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("KEY", "key");
    inputRecord.put("VALUE", "value");

    tester.pipeInput(inputTopic1, inputRecord);

    // when
    ReadOnlyKeyValueStore<Object, Object> store = tester.mockStreams()
        .store(storeName1, QueryableStoreTypes.keyValueStore());

    // then
    assertEquals("VALUE", store.get("key"));
  }

  @SneakyThrows
  @Test
  public void LocalDateParsedAsRelativeDate_t() {
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("nonNullableDate", "T");
    inputRecord.put("nonNullableStringField", "test");

    tester.pipeInput(inputTopic2, inputRecord);

    ReadOnlyKeyValueStore<TestMsgKey, TestMsgValue> store = tester.mockStreams()
        .store(storeName2, QueryableStoreTypes.keyValueStore());

    // then
    KeyValue<TestMsgKey, TestMsgValue> singleEntry = store.all().next();
    assertEquals(REF_DATE, singleEntry.value.getNonNullableDate());
  }

  @SneakyThrows
  @Test
  public void LocalDateParsedAsRelativeDate_tPlus1() {
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("nonNullableDate", "T+1");
    inputRecord.put("nonNullableStringField", "test");

    tester.pipeInput(inputTopic2, inputRecord);

    ReadOnlyKeyValueStore<TestMsgKey, TestMsgValue> store = tester.mockStreams()
        .store(storeName2, QueryableStoreTypes.keyValueStore());

    // then
    KeyValue<TestMsgKey, TestMsgValue> singleEntry = store.all().next();
    assertEquals(REF_DATE.plusDays(1), singleEntry.value.getNonNullableDate());
  }

  @SneakyThrows
  @Test
  public void LocalDateParsedAsRelativeDate_tMinus1() {
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("nonNullableDate", "T-1");
    inputRecord.put("nonNullableStringField", "test");

    tester.pipeInput(inputTopic2, inputRecord);

    ReadOnlyKeyValueStore<TestMsgKey, TestMsgValue> store = tester.mockStreams()
        .store(storeName2, QueryableStoreTypes.keyValueStore());

    // then
    KeyValue<TestMsgKey, TestMsgValue> singleEntry = store.all().next();
    assertEquals(REF_DATE.minusDays(1), singleEntry.value.getNonNullableDate());
  }

  @SneakyThrows
  @Test
  public void localDateParsedFromLong() {
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("nonNullableDate", "1");
    inputRecord.put("nonNullableStringField", "test");

    tester.pipeInput(inputTopic2, inputRecord);

    ReadOnlyKeyValueStore<TestMsgKey, TestMsgValue> store = tester.mockStreams()
        .store(storeName2, QueryableStoreTypes.keyValueStore());

    // then
    KeyValue<TestMsgKey, TestMsgValue> singleEntry = store.all().next();
    assertEquals(LocalDate.ofEpochDay(1), singleEntry.value.getNonNullableDate());
  }

  @Test
  @SneakyThrows
  public void primitiveKeyAndValue() {
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("KEY", "1");
    inputRecord.put("VALUE", "1");

    tester.pipeInput(inputTopic1, inputRecord);
  }

  @Test(expected = NullPointerException.class)
  @SneakyThrows
  public void primitiveKeyAndValue_MissingKeyEntry() {
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("VALUE", "1");

    tester.pipeInput(inputTopic1, inputRecord);
  }

  @Test(expected = NullPointerException.class)
  @SneakyThrows
  public void primitiveKeyAndValue_MissingValueEntry() {
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("KEY", "1");

    tester.pipeInput(inputTopic1, inputRecord);
  }

  @Test(expected = IllegalStateException.class)
  @SneakyThrows
  public void beanClassNotRegisteredCausesException() {
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("VALUE", "1");

    tester.pipeInput(inputTopic3, inputRecord);
  }

  @Test(expected = SerializationException.class)
  @SneakyThrows
  public void beanClassRegistered_NotAvro() {
    TopologyTester tester = newTesterWithoutAvroRegistered();
    tester.registerBeanClass(TestSubMsg.class);
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("nonNullableString", "a");

    tester.pipeInput(inputTopic3, inputRecord);
  }

  @Test
  @SneakyThrows
  public void classRegisteredAvroAndBean() {
    TopologyTester tester = newTester();
    tester.registerBeanClass(TestSubMsg.class);
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("nonNullableString", "a");

    tester.pipeInput(inputTopic3, inputRecord);
  }

  @Test
  @SneakyThrows
  public void beforeTestIsolatesTests() {
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("nonNullableStringField", "test");
    Map<String, String> storeRecord = new HashMap<>();
    storeRecord.put("nonNullableStringField", "TEST");

    tester.pipeInput(inputTopic2, inputRecord);
    tester.beforeTest();

    tester.assertOutputList(outputTopic2, Collections.emptyList(), false);
    tester.assertStoreNotContain(storeName2, Collections.singletonList(storeRecord));
  }

  @Test
  @SneakyThrows
  public void assertOutputAndStoreContain() {
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("nonNullableStringField", "test");
    Map<String, String> outputRecord = new HashMap<>();
    outputRecord.put("nonNullableStringField", "TEST");

    tester.pipeInput(inputTopic2, inputRecord);

    tester.assertOutputList(outputTopic2, Collections.singletonList(outputRecord), false);
    tester.assertStoreContain(storeName2, Collections.singletonList(outputRecord));
  }

  @Test
  @SneakyThrows
  public void assertOutputAndStoreContainWithDefaultField() {
    TopologyTester tester = newTester();
    tester.getOutputConfig(outputTopic2).getDefaultValues()
        .put("nullableStringField", "defaultVal");
    tester.getStoreConfig(storeName2).getDefaultValues().put("nullableStringField", "defaultVal");

    Set<String> extraFieldsToVerify = new HashSet<>();
    extraFieldsToVerify.add("nullableStringField");

    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("nonNullableStringField", "test");
    inputRecord.put("nullableStringField", "defaultVal");
    Map<String, String> outputRecord = new HashMap<>();
    outputRecord.put("nonNullableStringField", "TEST");

    tester.pipeInput(inputTopic2, inputRecord);

    tester.assertOutputList(outputTopic2, Collections.singletonList(outputRecord), false,
        extraFieldsToVerify);
    tester.assertStoreContain(storeName2, Collections.singletonList(outputRecord),
        extraFieldsToVerify);
  }

  @Test(expected = AssertionError.class)
  @SneakyThrows
  public void assertOutputListFailDefaultField() {
    TopologyTester tester = newTester();
    tester.getOutputConfig(outputTopic2).getDefaultValues()
        .put("nullableStringField", "defaultVal");

    Set<String> extraFieldsToVerify = new HashSet<>();
    extraFieldsToVerify.add("nullableStringField");

    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("nonNullableStringField", "test");
    Map<String, String> outputRecord = new HashMap<>();
    outputRecord.put("nonNullableStringField", "TEST");

    tester.pipeInput(inputTopic2, inputRecord);

    tester.assertOutputList(outputTopic2, Collections.singletonList(outputRecord), false,
        extraFieldsToVerify);
  }

  @Test(expected = AssertionError.class)
  @SneakyThrows
  public void assertOutputMapFailDefaultField() {
    TopologyTester tester = newTester();
    tester.getOutputConfig(outputTopic2).getDefaultValues()
        .put("nullableStringField", "defaultVal");

    Set<String> extraFieldsToVerify = new HashSet<>();
    extraFieldsToVerify.add("nullableStringField");

    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("nonNullableStringField", "test");
    Map<String, String> outputRecord = new HashMap<>();
    outputRecord.put("nonNullableStringField", "TEST");

    tester.pipeInput(inputTopic2, inputRecord);

    tester.assertOutputMap(outputTopic2, Collections.singletonList(outputRecord),
        extraFieldsToVerify);
  }

  @Test
  @SneakyThrows
  public void assertOutputMapDefaultField() {
    TopologyTester tester = newTester();
    tester.getOutputConfig(outputTopic2).getDefaultValues()
        .put("nullableStringField", "defaultVal");

    Set<String> extraFieldsToVerify = new HashSet<>();
    extraFieldsToVerify.add("nullableStringField");

    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("nonNullableStringField", "test");
    inputRecord.put("nullableStringField", "defaultVal");
    Map<String, String> outputRecord = new HashMap<>();
    outputRecord.put("nonNullableStringField", "TEST");

    tester.pipeInput(inputTopic2, inputRecord);

    tester.assertOutputMap(outputTopic2, Collections.singletonList(outputRecord),
        extraFieldsToVerify);
  }

  @Test(expected = AssertionError.class)
  @SneakyThrows
  public void assertStoreContainFailWithDefaultField() {
    TopologyTester tester = newTester();
    tester.getStoreConfig(storeName2).getDefaultValues().put("nullableStringField", "defaultVal");

    Set<String> extraFieldsToVerify = new HashSet<>();
    extraFieldsToVerify.add("nullableStringField");

    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("nonNullableStringField", "test");
    Map<String, String> outputRecord = new HashMap<>();
    outputRecord.put("nonNullableStringField", "TEST");

    tester.pipeInput(inputTopic2, inputRecord);

    tester.assertStoreContain(storeName2, Collections.singletonList(outputRecord),
        extraFieldsToVerify);
  }

  @Test
  @SneakyThrows
  public void assertStoreNotContain() {
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("nonNullableStringField", "test");
    Map<String, String> unexpectedRecord = new HashMap<>();
    unexpectedRecord.put("nonNullableStringField", "TEST2");

    tester.pipeInput(inputTopic2, inputRecord);

    tester.assertStoreNotContain(storeName2, Collections.singletonList(unexpectedRecord));
  }

  @Test
  @SneakyThrows
  public void purgeMessagesInOutput() {
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("nonNullableStringField", "test");

    tester.pipeInput(inputTopic2, inputRecord);
    tester.purgeMessagesInOutput(outputTopic2);

    tester.assertOutputList(outputTopic2, Collections.emptyList(), false);
  }

  @Test(expected = IllegalStateException.class)
  @SneakyThrows
  public void pipeInputWithoutConfiguring() {
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("nonNullableStringField", "test");
    Topic<TestMsgKey, TestMsgValue> unknownInputTopic = new Topic<>("UnknownTopic",
        inputTopic2.getKeySerde(), inputTopic2.getValueSerde());

    tester.pipeInput(unknownInputTopic, inputRecord);
  }

  @Test
  @SneakyThrows
  public void pipeInput_UseRecordCallback() {
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("nonNullableStringField", "test");

    Map<String, String> outputRecord = new HashMap<>();
    outputRecord.put("nonNullableStringField", "test");
    outputRecord.put("nullableStringField", "test");

    tester.pipeInput(inputTopic4, inputRecord, record -> {
      record.value.setNullableStringField(record.value.getNonNullableStringField());
      return record;
    });

    tester.assertOutputList(outputTopic4, Collections.singletonList(outputRecord), false);
  }

  @Test(expected = IllegalStateException.class)
  @SneakyThrows
  public void assertOutputWithoutConfiguring() {
    Map<String, String> outputRecord = new HashMap<>();
    outputRecord.put("nonNullableStringField", "test");
    Topic<TestMsgKey, TestMsgValue> unknownOutputTopic = new Topic<>("UnknownTopic",
        inputTopic2.getKeySerde(), inputTopic2.getValueSerde());

    tester.assertOutputList(unknownOutputTopic, Collections.singletonList(outputRecord), false);
  }

  @Test(expected = IllegalArgumentException.class)
  @SneakyThrows
  public void assertOutputList_MapsWithDifferentKeySets() {
    Map<String, String> inputRecord1 = new HashMap<>();
    inputRecord1.put("nonNullableStringField", "test");
    Map<String, String> inputRecord2 = new HashMap<>();
    inputRecord2.put("nonNullableStringField", "test");
    ArrayList<Map<String, String>> inputRecords = new ArrayList<>();
    inputRecords.add(inputRecord1);
    inputRecords.add(inputRecord2);

    Map<String, String> outputRecord1 = new HashMap<>();
    outputRecord1.put("nonNullableStringField", "test");
    Map<String, String> outputRecord2 = new HashMap<>();
    outputRecord2.put("nonNullableStringField", "test");
    outputRecord2.put("nullableStringField", "null");
    ArrayList<Map<String, String>> outputRecords = new ArrayList<>();
    outputRecords.add(outputRecord1);
    outputRecords.add(outputRecord2);

    // when
    tester.pipeInput(inputTopic2, inputRecords);

    // then
    tester.assertOutputList(outputTopic2, outputRecords, false);
  }

  @Test(expected = AssertionError.class)
  @SneakyThrows
  public void assertOutputList_AssertExpectedEmptyButNot() {
    Map<String, String> inputRecord1 = new HashMap<>();
    inputRecord1.put("nonNullableStringField", "test");

    // when
    tester.pipeInput(inputTopic2, Collections.singletonList(inputRecord1));

    // then
    tester.assertOutputList(outputTopic2, Collections.emptyList(), false);
  }

  @Test
  @SneakyThrows
  public void assertOutputList_UseAliasesAndConversions() {
    TopologyTester tester = newTester();
    String fieldAlias = "fieldA";
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("nonNullableStringField", "test___");

    Map<String, String> outputRecord = new HashMap<>();
    outputRecord.put(fieldAlias, "TEST");

    tester.getOutputConfig(outputTopic2).getAliases().put(fieldAlias, "nonNullableStringField");
    tester.getOutputConfig(outputTopic2).getConversions()
        .put(fieldAlias, stringValue -> stringValue + "___");

    // when
    tester.pipeInput(inputTopic2, Collections.singletonList(inputRecord));

    // then
    tester
        .assertOutputList(outputTopic2, Collections.singletonList(outputRecord), false);
  }

  @Test
  @SneakyThrows
  public void assertOutputMap_OnlyLatestPerKey() {
    Map<String, String> inputRecord1 = new HashMap<>();
    inputRecord1.put("keyValue", "1000");
    inputRecord1.put("nonNullableStringField", "test1");
    Map<String, String> inputRecord2 = new HashMap<>();
    inputRecord2.put("keyValue", "1000");
    inputRecord1.put("nonNullableStringField", "test2");
    List<Map<String, String>> inputRecords = new ArrayList<>();
    inputRecords.add(inputRecord1);
    inputRecords.add(inputRecord2);

    Map<String, String> outputRecord1 = new HashMap<>();
    outputRecord1.put("keyValue", "1000");
    outputRecord1.put("nonNullableStringField", "test2");

    Collection<Map<String, String>> expectedOutput = new HashSet<>();
    expectedOutput.add(outputRecord1);

    // when
    tester.pipeInput(inputTopic2, inputRecords);

    // then
    tester.assertOutputMap(outputTopic2, expectedOutput);
  }

  @Test
  @SneakyThrows
  public void assertOutputMap_OnlyLatestPerKeyIncludingUnexpectedFields() {
    Map<String, String> inputRecord1 = new HashMap<>();
    inputRecord1.put("keyValue", "1000");
    inputRecord1.put("doubleField", "1000");
    inputRecord1.put("nonNullableStringField", "test1");
    Map<String, String> inputRecord2 = new HashMap<>();
    inputRecord2.put("keyValue", "1000");
    inputRecord2.put("doubleField", "1001");    // differentiate input keys
    inputRecord2.put("nonNullableStringField", "test2");
    List<Map<String, String>> inputRecords = new ArrayList<>();
    inputRecords.add(inputRecord1);
    inputRecords.add(inputRecord2);

    Map<String, String> outputRecord1 = new HashMap<>();
    outputRecord1.put("keyValue", "1000");
    outputRecord1.put("nonNullableStringField", "TEST1");
    Map<String, String> outputRecord2 = new HashMap<>();
    outputRecord2.put("keyValue", "1000");
    outputRecord2.put("nonNullableStringField", "TEST2");
    Collection<Map<String, String>> expectedOutput = new ArrayList<>();
    expectedOutput.add(outputRecord1);
    expectedOutput.add(outputRecord2);

    // when
    tester.pipeInput(inputTopic2, inputRecords);

    // then
    tester.assertOutputMap(outputTopic2, expectedOutput);
  }

  @Test
  @SneakyThrows
  public void assertOutputList_OnlyCompareExpectedFields() {
    Map<String, String> inputRecord1 = new HashMap<>();
    inputRecord1.put("keyValue", "1000");
    inputRecord1.put("doubleField", "1000");
    inputRecord1.put("nonNullableStringField", "test1");
    Map<String, String> inputRecord2 = new HashMap<>();
    inputRecord2.put("keyValue", "1000");
    inputRecord2.put("doubleField", "1001");    // differentiate input keys
    inputRecord2.put("nonNullableStringField", "test2");
    List<Map<String, String>> inputRecords = new ArrayList<>();
    inputRecords.add(inputRecord1);
    inputRecords.add(inputRecord2);

    Map<String, String> outputRecord1 = new HashMap<>();
    outputRecord1.put("keyValue", "1000");
    List<Map<String, String>> expectedOutput = new ArrayList<>();
    expectedOutput.add(outputRecord1);
    expectedOutput.add(outputRecord1);

    // when
    tester.pipeInput(inputTopic2, inputRecords);

    // then
    tester.assertOutputList(outputTopic2, expectedOutput, false);
  }

  @Test
  @SneakyThrows
  public void assertOutputList_valueNull() {
    Map<String, String> inputRecord1 = new HashMap<>();
    inputRecord1.put("keyValue", "1000");
    inputRecord1.put("VALUE", "<null>");

    List<Map<String, String>> inputRecords = new ArrayList<>();
    inputRecords.add(inputRecord1);

    Map<String, String> outputRecord1 = new HashMap<>();
    outputRecord1.put("keyValue", "1000");
    outputRecord1.put("VALUE", "<null>");
    List<Map<String, String>> expectedOutput = new ArrayList<>();
    expectedOutput.add(outputRecord1);

    Map<String, String> storeRecord1 = new HashMap<>();
    storeRecord1.put("keyValue", "1000");

    // when
    tester.pipeInput(inputTopic4, inputRecords);

    // then
    tester.assertOutputList(outputTopic4, expectedOutput, false);
    tester.assertStoreNotContain(storeName4, Collections.singletonList(storeRecord1));
  }


  @Test
  @SneakyThrows
  public void testerDeletesStateDirAppId() {
    TopologyTester tester = newTester();
    long testerId = lastTesterId.get();

    Map<String, String> inputRecord1 = new HashMap<>();
    inputRecord1.put("nonNullableStringField", "fieldvalue");

    tester.pipeInput(inputTopic2, inputRecord1);
    tester.close();

    Properties newProps = streamProperties(testerId);
    tester = newTester(newProps);

    // then
    tester.assertOutputList(outputTopic2, Collections.emptyList(), false);
  }

  @Test(expected = IllegalArgumentException.class)
  @SneakyThrows
  public void pipeInputFailsOnMisspelledField() {
    Map<String, String> inputRecord1 = new HashMap<>();
    inputRecord1.put("nonNullableStringField_MISSPELLED", "fieldvalue");

    tester.pipeInput(inputTopic2, inputRecord1);
  }

  @Test(expected = IllegalArgumentException.class)
  @SneakyThrows
  public void assertOutputFailsOnMisspelledField() {
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("nonNullableStringField", "fieldvalue");
    Map<String, String> outputRecord = new HashMap<>();
    outputRecord.put("nonNullableStringField_MISSPELLED", "fieldvalue");

    tester.pipeInput(inputTopic2, inputRecord);

    tester.assertOutputList(outputTopic2, Collections.singletonList(outputRecord), false);
  }

  @Test
  @SneakyThrows
  public void assertOutputList_LenientOrderWithDuplicates() {
    Map<String, String> inputRecord1 = new HashMap<>();
    inputRecord1.put("doubleField", "123");
    inputRecord1.put("nonNullableStringField", "fieldvalue");
    Map<String, String> inputRecord2 = new HashMap<>();
    inputRecord2.put("doubleField", "1234");
    inputRecord2.put("nonNullableStringField", "fieldvalue2");

    Map<String, String> outputRecord1 = new HashMap<>();
    outputRecord1.put("nonNullableStringField", "fieldvalue");
    Map<String, String> outputRecord2 = new HashMap<>();
    outputRecord2.put("nonNullableStringField", "fieldvalue2");

    List<Map<String, String>> outputRecords = new ArrayList<>();
    outputRecords.add(outputRecord1);
    outputRecords.add(outputRecord1);
    outputRecords.add(outputRecord2);

    tester.pipeInput(inputTopic4, inputRecord1);
    tester.pipeInput(inputTopic4, inputRecord1);
    tester.pipeInput(inputTopic4, inputRecord2);

    tester.assertOutputList(outputTopic4, outputRecords, true);
  }

  @Test(expected = AssertionError.class)
  @SneakyThrows
  public void assertOutputList_LenientOrderWithDuplicatesFails() {
    Map<String, String> inputRecord1 = new HashMap<>();
    inputRecord1.put("doubleField", "123");
    inputRecord1.put("nonNullableStringField", "fieldvalue");
    Map<String, String> inputRecord2 = new HashMap<>();
    inputRecord2.put("doubleField", "1234");
    inputRecord2.put("nonNullableStringField", "fieldvalue2");

    Map<String, String> outputRecord1 = new HashMap<>();
    outputRecord1.put("nonNullableStringField", "fieldvalue");
    Map<String, String> outputRecord2 = new HashMap<>();
    outputRecord2.put("nonNullableStringField", "fieldvalue3");

    List<Map<String, String>> outputRecords = new ArrayList<>();
    outputRecords.add(outputRecord1);
    outputRecords.add(outputRecord1);
    outputRecords.add(outputRecord2);

    tester.pipeInput(inputTopic4, inputRecord1);
    tester.pipeInput(inputTopic4, inputRecord1);
    tester.pipeInput(inputTopic4, inputRecord2);

    tester.assertOutputList(outputTopic4, outputRecords, true);
  }

  @Test
  @SneakyThrows
  public void assertOutputListWithDefaultFields() {

    TopologyTester tester = newTester();
    TopicConfig<TestMsgKey, TestMsgValue> outputConfig = tester.getOutputConfig(outputTopic4);
    outputConfig.getDefaultValues().put("nullableStringField", "defaultVal");

    Set<String> extraFieldsToValidate = new HashSet<>();
    extraFieldsToValidate.add("nullableStringField");

    Map<String, String> inputRecord1 = new HashMap<>();
    inputRecord1.put("doubleField", "123");
    inputRecord1.put("nonNullableStringField", "fieldvalue");
    inputRecord1.put("nullableStringField", "defaultVal");
    Map<String, String> inputRecord2 = new HashMap<>();
    inputRecord2.put("doubleField", "1234");
    inputRecord2.put("nonNullableStringField", "fieldvalue2");
    inputRecord2.put("nullableStringField", "defaultVal");

    Map<String, String> outputRecord1 = new HashMap<>();
    outputRecord1.put("nonNullableStringField", "fieldvalue");
    Map<String, String> outputRecord2 = new HashMap<>();
    outputRecord2.put("nonNullableStringField", "fieldvalue2");

    List<Map<String, String>> outputRecords = new ArrayList<>();
    outputRecords.add(outputRecord1);
    outputRecords.add(outputRecord1);
    outputRecords.add(outputRecord2);

    tester.pipeInput(inputTopic4, inputRecord1);
    tester.pipeInput(inputTopic4, inputRecord1);
    tester.pipeInput(inputTopic4, inputRecord2);

    tester.assertOutputList(outputTopic4, outputRecords, true, extraFieldsToValidate);
  }

  @Test(expected = IllegalArgumentException.class)
  @SneakyThrows
  public void assertOutputList_ExceptionWithExtraFieldButNoDefault() {

    Set<String> extraFieldsToValidate = new HashSet<>();
    extraFieldsToValidate.add("nullableStringField");

    Map<String, String> inputRecord1 = new HashMap<>();
    inputRecord1.put("doubleField", "123");
    inputRecord1.put("nonNullableStringField", "fieldvalue");
    inputRecord1.put("nullableStringField", "defaultVal");

    Map<String, String> outputRecord1 = new HashMap<>();
    outputRecord1.put("nonNullableStringField", "fieldvalue");

    List<Map<String, String>> outputRecords = new ArrayList<>();
    outputRecords.add(outputRecord1);

    tester.pipeInput(inputTopic4, inputRecord1);

    tester.assertOutputList(outputTopic4, outputRecords, true, extraFieldsToValidate);
  }

  @Test(expected = IllegalArgumentException.class)
  @SneakyThrows
  public void assertOutputMap_ExceptionWithExtraFieldButNoDefault() {

    Set<String> extraFieldsToValidate = new HashSet<>();
    extraFieldsToValidate.add("nullableStringField");

    Map<String, String> inputRecord1 = new HashMap<>();
    inputRecord1.put("doubleField", "123");
    inputRecord1.put("nonNullableStringField", "fieldvalue");
    inputRecord1.put("nullableStringField", "defaultVal");

    Map<String, String> outputRecord1 = new HashMap<>();
    outputRecord1.put("nonNullableStringField", "fieldvalue");

    List<Map<String, String>> outputRecords = new ArrayList<>();
    outputRecords.add(outputRecord1);

    tester.pipeInput(inputTopic4, inputRecord1);

    tester.assertOutputMap(outputTopic4, outputRecords, extraFieldsToValidate);
  }

  @Test(expected = IllegalArgumentException.class)
  @SneakyThrows
  public void assertStoreContain_ExceptionWithExtraFieldButNoDefault() {

    Set<String> extraFieldsToValidate = new HashSet<>();
    extraFieldsToValidate.add("nullableStringField");

    Map<String, String> inputRecord1 = new HashMap<>();
    inputRecord1.put("nonNullableStringField", "fieldvalue");
    inputRecord1.put("nullableStringField", "defaultVal");

    Map<String, String> outputRecord1 = new HashMap<>();
    outputRecord1.put("nonNullableStringField", "fieldvalue");

    List<Map<String, String>> outputRecords = new ArrayList<>();
    outputRecords.add(outputRecord1);

    tester.pipeInput(inputTopic4, inputRecord1);

    tester.assertStoreContain(storeName4, outputRecords, extraFieldsToValidate);
  }

  @Test
  @SneakyThrows
  public void assertStoreContain_ExtraField() {

    TopologyTester tester = newTester();
    tester.getStoreConfig(storeName4).getDefaultValues().put("nullableStringField", "defaultVal");

    Set<String> extraFieldsToValidate = new HashSet<>();
    extraFieldsToValidate.add("nullableStringField");

    Map<String, String> inputRecord1 = new HashMap<>();
    inputRecord1.put("nonNullableStringField", "fieldvalue");
    inputRecord1.put("nullableStringField", "defaultVal");

    Map<String, String> outputRecord1 = new HashMap<>();
    outputRecord1.put("nonNullableStringField", "fieldvalue");

    List<Map<String, String>> outputRecords = new ArrayList<>();
    outputRecords.add(outputRecord1);

    tester.pipeInput(inputTopic4, inputRecord1);

    tester.assertStoreContain(storeName4, outputRecords, extraFieldsToValidate);
  }

  @Test(expected = IllegalArgumentException.class)
  @SneakyThrows
  public void assertStoreNotContain_ExceptionWithExtraFieldButNoDefault() {

    Set<String> extraFieldsToValidate = new HashSet<>();
    extraFieldsToValidate.add("nullableStringField");

    Map<String, String> outputRecord1 = new HashMap<>();
    outputRecord1.put("nonNullableStringField", "fieldvalue");

    List<Map<String, String>> outputRecords = new ArrayList<>();
    outputRecords.add(outputRecord1);

    tester.assertStoreNotContain(storeName4, outputRecords, extraFieldsToValidate);
  }

  @Test
  @SneakyThrows
  public void assertStoreNotContain_ExtraField() {

    TopologyTester tester = newTester();
    tester.getStoreConfig(storeName4).getDefaultValues().put("nullableStringField", "defaultVal");

    Set<String> extraFieldsToValidate = new HashSet<>();
    extraFieldsToValidate.add("nullableStringField");

    Map<String, String> outputRecord1 = new HashMap<>();
    outputRecord1.put("nonNullableStringField", "fieldvalue");

    List<Map<String, String>> outputRecords = new ArrayList<>();
    outputRecords.add(outputRecord1);

    tester.assertStoreNotContain(storeName4, outputRecords, extraFieldsToValidate);
  }

}