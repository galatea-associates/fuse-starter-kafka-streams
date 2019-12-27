package org.galatea.kafka.starter.testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.SneakyThrows;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.test.TestMsgKey;
import org.galatea.kafka.starter.messaging.test.TestMsgValue;
import org.junit.Before;
import org.junit.Test;

public class TopologyTesterTest {

  private static TopologyTester tester;
  private static final Topology testTopology = TestTopology.topology();
  private static final Topic<String, String> inputTopic1 = TestTopology.inputTopic;
  private static final Topic<TestMsgKey, TestMsgValue> inputTopic2 = TestTopology.inputTopic2;
  private static final Topic<String, String> outputTopic1 = TestTopology.outputTopic;
  private static final Topic<TestMsgKey, TestMsgValue> outputTopic2 = TestTopology.outputTopic2;
  private static final String storeName1 = TestTopology.STORE_NAME1;
  private static final String storeName2 = TestTopology.STORE_NAME2;
  private static final Properties streamProperties;

  static {
    streamProperties = new Properties();
    streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app.id");
    streamProperties.put(StreamsConfig.STATE_DIR_CONFIG, "target/testStateDir");
    streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:65535");
  }

  @Before
  public void setup() {
    if (tester == null) {
      tester = new TopologyTester(testTopology, streamProperties);

      tester.configureInputTopic(inputTopic1, String::new, String::new);
      tester.configureInputTopic(inputTopic2, TestMsgKey::new, TestMsgValue::new);
      tester.configureOutputTopic(outputTopic1, String::new, String::new);
      tester.configureOutputTopic(outputTopic2, TestMsgKey::new, TestMsgValue::new);

      tester.configureStore(storeName1, Serdes.String(), Serdes.String(), String::new, String::new);
      tester.configureStore(storeName2, inputTopic2.getKeySerde(), inputTopic2.getValueSerde(),
          TestMsgKey::new, TestMsgValue::new);

      tester.registerAvroClass(SpecificRecord.class);
      tester.registerBeanClass(SpecificRecord.class);
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
  public void stateStoreDeletedOnConstructor() {
    Map<String, String> inputRecord = new HashMap<>();
    inputRecord.put("KEY", "key");
    inputRecord.put("VALUE", "value");

    tester.pipeInput(inputTopic1, inputRecord);

    tester.close();
    tester = new TopologyTester(testTopology, streamProperties);

    ReadOnlyKeyValueStore<Object, Object> store = tester.mockStreams()
        .store(storeName1, QueryableStoreTypes.keyValueStore());

    // then
    assertNull(store.get("key"));
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
    assertEquals(TopologyTester.REF_DATE, singleEntry.value.getNonNullableDate());
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
    assertEquals(TopologyTester.REF_DATE.plusDays(1), singleEntry.value.getNonNullableDate());
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
    assertEquals(TopologyTester.REF_DATE.minusDays(1), singleEntry.value.getNonNullableDate());
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


  /*
   * TODO: add tests for the following scenarios:
   *  registering bean classes causes different treatment
   *  registering avro classes causes different treatment
   *  beforeTest wipes output and stores
   *  purgeMessagesInOutput wipes output
   *  trying to pipeInput without configuring causes exception
   *  trying to assertOutput without configuring causes exception
   *  assertOutputList throws exception if maps contain different key sets
   *  assertOutputList fails when expected is empty but output is not
   *  assertOutputList uses aliases and conversions
   *  assertOutputMap only has latest for each key
   *  assertStoreContain
   *  assertStoreNotContain
   *  assertOutputList only compares fields expected to be in output
   * */
}