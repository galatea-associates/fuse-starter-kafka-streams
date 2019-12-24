package org.galatea.kafka.starter.testing;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.unitils.reflectionassert.ReflectionAssert.assertReflectionEquals;

import java.io.File;
import java.nio.file.FileSystem;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field.Str;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.testing.alias.AliasHelper;
import org.galatea.kafka.starter.testing.avro.AvroMessageUtil;
import org.galatea.kafka.starter.testing.bean.RecordBeanHelper;
import org.springframework.util.FileSystemUtils;
import org.springframework.util.ReflectionUtils;
import org.unitils.reflectionassert.ReflectionAssert;
import org.unitils.reflectionassert.ReflectionComparatorMode;

@Slf4j
public class TopologyTester {

  @Getter
  private final TopologyTestDriver driver;
  private final Map<String, TopicConfig<?, ?>> inputTopicConfig = new HashMap<>();
  private final Map<String, TopicConfig<?, ?>> outputTopicConfig = new HashMap<>();
  private final Map<String, TopicConfig<?, ?>> storeConfig = new HashMap<>();

  public TopologyTester(Topology topology, Properties streamProperties) {
    String stateDir = streamProperties.getProperty(StreamsConfig.STATE_DIR_CONFIG);
    File dirFile = new File(stateDir);
    if (dirFile.exists()) {
      log.info("Able to delete state dir before testing: {}",
          FileSystemUtils.deleteRecursively(dirFile));
    }
    driver = new TopologyTestDriver(topology, streamProperties);

  }

  public void beforeTest() {
    for (Entry<String, StateStore> e : driver.getAllStateStores().entrySet()) {
      String storeName = e.getKey();
      StateStore store = e.getValue();
      KeyValueStore<Object, ?> kvStore = (KeyValueStore<Object, ?>) store;
      try (KeyValueIterator<Object, ?> iter = kvStore.all()) {
        while (iter.hasNext()) {
          KeyValue<Object, ?> entry = iter.next();
          log.info("Deleting entry in {}: {}", storeName, entry);
          kvStore.delete(entry.key);
        }
      }
    }
  }

  public <K, V> void configureInputTopic(Topic<K, V> topic,
      Callable<K> createEmptyKey, Callable<V> createEmptyValue) {
    inputTopicConfig.put(topic.getName(),
        new TopicConfig<>(topic.getName(), topic.getKeySerde(), topic.getValueSerde(),
            createEmptyKey, createEmptyValue));
  }

  public <K, V> void configureOutputTopic(Topic<K, V> topic,
      Callable<K> createEmptyKey, Callable<V> createEmptyValue) {
    outputTopicConfig.put(topic.getName(),
        new TopicConfig<>(topic.getName(), topic.getKeySerde(), topic.getValueSerde(),
            createEmptyKey, createEmptyValue));
  }

  public <K, V> void configureStore(String storeName, Serde<K> keySerde, Serde<V> valueSerde,
      Callable<K> createEmptyKey, Callable<V> createEmptyValue) {
    outputTopicConfig.put(storeName,
        new TopicConfig<>(storeName, keySerde, valueSerde, createEmptyKey, createEmptyValue));
  }

  public <K, V> void pipeInput(Topic<K, V> topic, List<Map<String, String>> records)
      throws Exception {
    for (Map<String, String> record : records) {
      pipeInput(topic, record);
    }
  }

  public <K, V> void pipeInput(Topic<K, V> topic, Map<String, String> fieldMap) throws Exception {
    TopicConfig<K, V> topicConfig = inputTopicConfig(topic);

    KeyValue<K, V> record = RecordBeanHelper.createRecord(fieldMap, topicConfig);
    AvroMessageUtil.defaultUtil().populateRequiredFieldsWithDefaults((SpecificRecord) record.key);
    AvroMessageUtil.defaultUtil().populateRequiredFieldsWithDefaults((SpecificRecord) record.value);

    driver.pipeInput(topicConfig.factory().create(Collections.singletonList(record)));
  }

  /**
   * maps within list of expected records may NOT have different key sets
   */
  public <K, V> void assertOutput(Topic<K, V> topic, List<Map<String, String>> expectedRecords,
      boolean lenientOrder) throws Exception {
    TopicConfig<K, V> topicConfig = outputTopicConfig(topic);

    List<KeyValue<K, V>> actualOutput = readOutput(topicConfig);
    if (!actualOutput.isEmpty()) {
      assertFalse("actualOutput is not empty but expectedOutput is. At least 1 record is required "
          + "in 'expectedRecords' for in-depth comparison", expectedRecords.isEmpty());
    }
    Set<String> expectedFields = AliasHelper
        .expandAliasKeys(expectedRecords.get(0).keySet(), topicConfig.getAliases());

    List<KeyValue<K, V>> comparableActualOutput = new ArrayList<>();
    for (KeyValue<K, V> actualOutputRecord : actualOutput) {
      // copy properties from actual output records into new records that ONLY have those fields set
      comparableActualOutput.add(RecordBeanHelper
          .copyRecordPropertiesIntoNew(expectedFields, actualOutputRecord, topicConfig,
              RecordBeanHelper.PREFIX_KEY, RecordBeanHelper.PREFIX_VALUE));
    }

    List<KeyValue<K, V>> expectedOutput = new ArrayList<>();
    for (Map<String, String> expectedRecordMap : expectedRecords) {
      if (!expectedRecordMap.keySet().equals(expectedFields)) {
        throw new IllegalArgumentException(String.format("Expected records (as maps) have "
                + "differing key sets.\n\tExpected: %s\n\tActual: %s", expectedFields,
            expectedRecordMap.keySet()));
      }

      expectedOutput.add(RecordBeanHelper.copyRecordPropertiesIntoNew(expectedFields,
          RecordBeanHelper.createRecord(expectedRecordMap, topicConfig), topicConfig,
          RecordBeanHelper.PREFIX_KEY, RecordBeanHelper.PREFIX_VALUE));
    }

    assertListEquals(expectedOutput, comparableActualOutput, lenientOrder);
  }

  private void assertListEquals(List<?> expected, List<?> actual,
      boolean lenientOrder) {
    try {
      if (lenientOrder) {
        assertReflectionEquals(expected, actual,
            ReflectionComparatorMode.LENIENT_ORDER);
      } else {
        assertReflectionEquals(expected, actual);
      }
    } catch (IllegalStateException e) {
      if (!lenientOrder) {
        throw e;    // believe the IllegalStateException will only occur with LENIENT_ORDER enabled
      }
      List<?> actualWithoutExpected = new ArrayList<>(actual);

      StringBuilder sb = new StringBuilder("Expected and Actual lists do no match:");
      for (Object expectedEntry : expected) {
        if (actualWithoutExpected.contains(expectedEntry)) {
          actualWithoutExpected.remove(expectedEntry);
        } else {
          sb.append("\n\tExpected but not received: ").append(expectedEntry.toString());
        }
      }
      for (Object receivedButNotExpected : actualWithoutExpected) {
        sb.append("\n\tReceived but not expected: ").append(receivedButNotExpected.toString());
      }
      fail(sb.toString());
    }
  }

  private <K, V> List<KeyValue<K, V>> readOutput(TopicConfig<K, V> config) {
    List<KeyValue<K, V>> outputList = new ArrayList<>();
    ProducerRecord<K, V> record;
    do {
      record = driver.readOutput(config.getTopicName(), config.getKeySerde().deserializer(),
          config.getValueSerde().deserializer());
      if (record != null) {
        outputList.add(new KeyValue<>(record.key(), record.value()));
      }
    } while (record != null);

    return outputList;
  }

  private <K, V> TopicConfig<K, V> inputTopicConfig(Topic<K, V> topic) {
    TopicConfig<?, ?> topicConfig = inputTopicConfig.get(topic.getName());
    if (topicConfig == null) {
      throw new IllegalStateException(
          String.format("Input topic %s is not configured", topic.getName()));
    }
    return (TopicConfig<K, V>) topicConfig;
  }

  private <K, V> TopicConfig<K, V> outputTopicConfig(Topic<K, V> topic) {
    TopicConfig<?, ?> topicConfig = outputTopicConfig.get(topic.getName());
    if (topicConfig == null) {
      throw new IllegalStateException(
          String.format("Output topic %s is not configured", topic.getName()));
    }
    return (TopicConfig<K, V>) topicConfig;
  }

  private <K, V> TopicConfig<K, V> storeConfig(String storeName) {
    TopicConfig<?, ?> topicConfig = storeConfig.get(storeName);
    if (topicConfig == null) {
      throw new IllegalStateException(
          String.format("Store %s is not configured", storeName));
    }
    return (TopicConfig<K, V>) topicConfig;
  }

//  public void test() throws Exception {
//
//    TopicConfig<TradeMsgKey, TestMsg> topicConfig = new TopicConfig<>(TradeMsgKey::new,
//        TestMsg::new);
//
//    Map<String, String> fieldMap = new HashMap<>();
//    fieldMap.put("tradeId", "100d");
//    fieldMap.put("value.nonNullableTimestamp", "2019-12-20T10:43:00.000Z");
//    fieldMap.put("nonNullableDate", "2019-12-20");
//
//    KeyValue<TradeMsgKey, TestMsg> record = RecordBeanHelper.createRecord(fieldMap, topicConfig);
//    log.info("Record: {}", record);
//  }


}
