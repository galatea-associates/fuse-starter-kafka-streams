package org.galatea.kafka.starter.testing;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.unitils.reflectionassert.ReflectionAssert.assertReflectionEquals;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.MockCluster;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.testing.alias.AliasHelper;
import org.galatea.kafka.starter.testing.avro.RecordPostProcessor;
import org.galatea.kafka.starter.testing.conversion.ConversionService;
import org.springframework.util.FileSystemUtils;
import org.unitils.reflectionassert.ReflectionComparatorMode;

@Slf4j
public class TopologyTester implements Closeable {

  @Getter
  private final TopologyTestDriver driver;
  private final Map<String, TopicConfig<?, ?>> inputTopicConfig = new HashMap<>();
  private final Map<String, TopicConfig<?, ?>> outputTopicConfig = new HashMap<>();
  private final Map<String, TopicConfig<?, ?>> storeConfig = new HashMap<>();
  private final Set<Class<?>> beanClasses = new HashSet<>();

  @Getter
  private final ConversionService typeConversionService = new ConversionService();
  private final Map<Class<?>, RecordPostProcessor<?>> postProcessors = new HashMap<>();

  /**
   * Use this KafkaStreams object for calling any code that needs to retrieve stores from the
   * KafkaStreams object
   */
  public KafkaStreams mockStreams() {
    KafkaStreams mockStreams = mock(KafkaStreams.class);
    when(mockStreams.store(any(String.class), any(QueryableStoreType.class)))
        .thenAnswer(invocationOnMock -> driver.getKeyValueStore(invocationOnMock.getArgument(0)));
    return mockStreams;
  }

  public <T> void registerPostProcessor(Class<T> forClass, RecordPostProcessor<T> processor) {
    postProcessors.put(forClass, processor);
  }

  public TopologyTester(Topology topology, Properties streamProperties) {
    String stateDir =
        streamProperties.getProperty(StreamsConfig.STATE_DIR_CONFIG) + "/" + streamProperties
            .getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
    File dirFile = new File(stateDir);
    if (dirFile.exists() && !FileSystemUtils.deleteRecursively(dirFile)) {
      log.error("Was unable to delete state dir before tests: {}", stateDir);
    }
    driver = new TopologyTestDriver(topology, streamProperties, MockCluster.empty());
    log.info("Initiated new TopologyTester with application ID: {}",
        streamProperties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));
  }

  /**
   * Register class/interface to be treated as a bean. If not registered, the class will be created
   * using the string constructor if it exists.
   */
  public void registerBeanClass(Class<?> beanClassOrInterface) {
    beanClasses.add(beanClassOrInterface);
  }

  public void beforeTest() {
    outputTopicConfig.forEach((topicName, topicConfig) -> readOutput(topicConfig));

    driver.getAllStateStores().values().stream().flatMap(Collection::stream)
        .map(s -> (KeyValueStore<Object, ?>) s)
        .forEach(kvStore -> {
          try (KeyValueIterator<Object, ?> iter = kvStore.all()) {
            while (iter.hasNext()) {
              KeyValue<Object, ?> entry = iter.next();
              log.info("Deleting entry in store: {}", entry);
              kvStore.delete(entry.key);
            }

            kvStore.flush();
          }
        });

  }

  public <K, V> void purgeMessagesInOutput(Topic<K, V> topic) {
    readOutput(outputTopicConfig(topic));
  }

  @SuppressWarnings("unchecked")
  public <K, V> TopicConfig<K, V> getInputConfig(Topic<K, V> topic) {
    return (TopicConfig<K, V>) inputTopicConfig.get(topic.getName());
  }

  @SuppressWarnings("unchecked")
  public <K, V> TopicConfig<K, V> getOutputConfig(Topic<K, V> topic) {
    return (TopicConfig<K, V>) outputTopicConfig.get(topic.getName());
  }

  @SuppressWarnings("unchecked")
  public <K, V> TopicConfig<K, V> getStoreConfig(String storeName) {
    return (TopicConfig<K, V>) storeConfig.get(storeName);
  }

  public <K, V> TopicConfig<K, V> configureInputTopic(Topic<K, V> topic,
      Callable<K> createEmptyKey, Callable<V> createEmptyValue) {

    if (inputTopicConfig.containsKey(topic.getName())) {
      throw new IllegalStateException(
          String.format("Input topic %s cannot be configured more than once", topic.getName()));
    }
    inputTopicConfig.put(topic.getName(),
        new TopicConfig<>(topic.getName(), topic.getKeySerde(), topic.getValueSerde(),
            createEmptyKey, createEmptyValue, testInputTopic(topic)));
    return inputTopicConfig(topic);
  }

  private <K, V> TestInputTopic<K, V> testInputTopic(Topic<K, V> topic) {
    return driver.createInputTopic(topic.getName(), topic.getKeySerde().serializer(),
        topic.getValueSerde().serializer());
  }

  private <K, V> TestOutputTopic<K, V> testOutputTopic(Topic<K, V> topic) {
    return driver.createOutputTopic(topic.getName(), topic.getKeySerde().deserializer(),
        topic.getValueSerde().deserializer());
  }

  public <K, V> TopicConfig<K, V> configureOutputTopic(Topic<K, V> topic,
      Callable<K> createEmptyKey, Callable<V> createEmptyValue) {
    if (outputTopicConfig.containsKey(topic.getName())) {
      throw new IllegalStateException(
          String.format("Output topic %s cannot be configured more than once", topic.getName()));
    }
    outputTopicConfig.put(topic.getName(),
        new TopicConfig<>(topic.getName(), topic.getKeySerde(), topic.getValueSerde(),
            createEmptyKey, createEmptyValue, testOutputTopic(topic)));
    return outputTopicConfig(topic);
  }

  public <K, V> TopicConfig<K, V> configureStore(String storeName, Serde<K> keySerde,
      Serde<V> valueSerde,
      Callable<K> createEmptyKey, Callable<V> createEmptyValue) {
    if (storeConfig.containsKey(storeName)) {
      throw new IllegalStateException(
          String.format("Store %s cannot be configured more than once", storeName));
    }
    storeConfig.put(storeName,
        new TopicConfig<>(storeName, keySerde, valueSerde, createEmptyKey, createEmptyValue));
    return storeConfig(storeName);
  }

  public <K, V> void pipeInput(Topic<K, V> topic, List<Map<String, String>> records)
      throws Exception {
    pipeInput(topic, records, null);
  }

  public <K, V> void pipeInput(Topic<K, V> topic, List<Map<String, String>> records,
      Function<KeyValue<K, V>, KeyValue<K, V>> recordCreationCallback)
      throws Exception {
    for (Map<String, String> record : records) {
      pipeInput(topic, record, recordCreationCallback);
    }
  }

  public <K, V> void pipeInput(Topic<K, V> topic, Map<String, String> fieldMap) throws Exception {
    pipeInput(topic, fieldMap, null);
  }

  public <K, V> void pipeInput(Topic<K, V> topic, Map<String, String> fieldMap,
      Function<KeyValue<K, V>, KeyValue<K, V>> recordCreationCallback) throws Exception {
    TopicConfig<K, V> topicConfig = inputTopicConfig(topic);

    KeyValue<K, V> record = createRecordWithProcessing(fieldMap, topicConfig);

    if (recordCreationCallback != null) {
      record = recordCreationCallback.apply(record);
    }
    log.info("{} Piping record into topology on topic {}: {}", TopologyTester.class.getSimpleName(),
        topic.getName(), record);
    topicConfig.getConfiguredInput().pipeInput(record.key, record.value);
  }

  private <V, K> boolean keyIsBean(TopicConfig<K, V> topicConfig) throws Exception {
    Class<?> keyClass = topicConfig.createKey().getClass();
    return beanClasses.contains(keyClass) || collectionContainsAny(beanClasses,
        Arrays.asList(keyClass.getInterfaces()));
  }

  private <V, K> boolean valueIsBean(TopicConfig<K, V> topicConfig) throws Exception {
    Class<?> valueClass = topicConfig.createValue().getClass();
    return beanClasses.contains(valueClass) || collectionContainsAny(beanClasses,
        Arrays.asList(valueClass.getInterfaces()));
  }

  public static <T> boolean collectionContainsAny(Set<T> set, Collection<T> contains) {
    for (T contain : contains) {
      if (set.contains(contain)) {
        return true;
      }
    }
    return false;
  }

  public <K, V> void assertOutputList(Topic<K, V> topic, List<Map<String, String>> expectedRecords,
      boolean lenientOrder) throws Exception {
    assertOutputList(topic, expectedRecords, lenientOrder, Collections.emptySet());
  }

  /**
   * maps within list of expected records may NOT have different key sets
   */
  public <K, V> void assertOutputList(Topic<K, V> topic, List<Map<String, String>> expectedRecords,
      boolean lenientOrder, Set<String> extraFieldsToCompareWithDefaults) throws Exception {
    TopicConfig<K, V> topicConfig = outputTopicConfig(topic);

    List<KeyValue<K, V>> output = readOutput(topicConfig);
    if (expectedRecords.isEmpty() && output.isEmpty()) {
      return;
    }

    if (!output.isEmpty()) {
      assertFalse("output is not empty but expectedOutput is. At least 1 record is required "
          + "in 'expectedRecords' for in-depth comparison", expectedRecords.isEmpty());
    }
    Set<String> expectedFields = AliasHelper
        .expandAliasKeys(expectedRecords.get(0).keySet(), topicConfig.getAliases());
    expectedFields.addAll(
        AliasHelper.expandAliasKeys(extraFieldsToCompareWithDefaults, topicConfig.getAliases()));

    // comparableActualOutput has only necessary fields populated, as defined by 'expectedFields'
    List<KeyValue<K, V>> comparableActualOutput = stripUnnecessaryFields(output,
        expectedFields, topicConfig);

    List<KeyValue<K, V>> expectedOutput = new ArrayList<>(
        expectedRecordsFromMaps(topicConfig, expectedRecords,
            extraFieldsToCompareWithDefaults));

    assertListEquals(expectedOutput, comparableActualOutput, lenientOrder);
  }

  protected <V, K> Collection<KeyValue<K, V>> expectedRecordsFromMaps(TopicConfig<K, V> topicConfig,
      Collection<Map<String, String>> expectedRecordMaps, Set<String> extraFieldsToDefault)
      throws Exception {

    List<KeyValue<K, V>> expectedOutput = new ArrayList<>();
    if (expectedRecordMaps.isEmpty()) {
      return expectedOutput;
    }

    Set<String> expectedFields = expectedRecordMaps.stream().findFirst().map(Map::keySet)
        .orElse(new HashSet<>());

    // verify that all maps in expectedRecordMaps have the same set of fields
    for (Map<String, String> expectedRecordMap : expectedRecordMaps) {
      if (!expectedRecordMap.keySet().equals(expectedFields)) {
        throw new IllegalArgumentException(String.format("Expected records (as maps) have "
                + "differing key sets.\n\tExpected: %s\n\tActual: %s", expectedFields,
            expectedRecordMap.keySet()));
      }
    }

    Set<String> fieldsToMakeDefault = AliasHelper
        .expandAliasKeys(extraFieldsToDefault, topicConfig.getAliases());
    Map<String, String> topicDefaultValues = AliasHelper
        .expandAliasKeys(topicConfig.getDefaultValues(), topicConfig.getAliases());

    for (Map<String, String> expectedRecordMap : expectedRecordMaps) {
      expectedRecordMap = AliasHelper.expandAliasKeys(expectedRecordMap, topicConfig.getAliases());

      // add default values to expectedRecordMap if the field doesn't exist already
      for (String fieldToDefault : fieldsToMakeDefault) {
        expectedRecordMap.computeIfAbsent(fieldToDefault,
            key -> Optional.ofNullable(topicDefaultValues.get(key)).orElseThrow(
                () -> new IllegalArgumentException(String.format(
                    "Cannot do an explicit compare on field '%s' because default value is not set",
                    fieldToDefault))));
      }

      expectedOutput.add(createRecordWithProcessing(expectedRecordMap, topicConfig));
    }

    return expectedOutput;
  }

  public void assertStoreContain(String storeName, Collection<Map<String, String>> expected)
      throws Exception {
    assertStoreContain(storeName, expected, Collections.emptySet());
  }

  public void assertStoreContain(String storeName, Collection<Map<String, String>> expected,
      Set<String> extraFieldsToDefault) throws Exception {
    TopicConfig<Object, Object> storeConfig = storeConfig(storeName);

    if (expected.isEmpty()) {
      return;
    }

    KeyValueStore<Object, Object> store = driver.getKeyValueStore(storeName);

    Collection<KeyValue<Object, Object>> expectedRecords = expectedRecordsFromMaps(storeConfig,
        expected, extraFieldsToDefault);

    Set<String> expectedFields = expected.iterator().next().keySet();
    expectedFields = AliasHelper.expandAliasKeys(expectedFields, storeConfig.getAliases());
    expectedFields
        .addAll(AliasHelper.expandAliasKeys(extraFieldsToDefault, storeConfig.getAliases()));

    List<KeyValue<Object, Object>> storeContentsStripped = new ArrayList<>();
    try (KeyValueIterator<Object, Object> iter = store.all()) {
      while (iter.hasNext()) {
        storeContentsStripped.add(stripUnnecessaryFields(iter.next(), expectedFields, storeConfig));
      }
    }

    for (KeyValue<Object, Object> expectedRecord : expectedRecords) {
      assertTrue(String.format("Store does not contain record with matching fields %s; values: %s",
          expectedFields.toString(), expectedRecord),
          storeContentsStripped.contains(expectedRecord));
    }
  }

  public void assertStoreNotContain(String storeName, Collection<Map<String, String>> unexpected)
      throws Exception {
    assertStoreNotContain(storeName, unexpected, Collections.emptySet());
  }

  public void assertStoreNotContain(String storeName, Collection<Map<String, String>> unexpected,
      Set<String> extraFieldsToDefault) throws Exception {
    TopicConfig<Object, Object> storeConfig = storeConfig(storeName);

    if (unexpected.isEmpty()) {
      return;
    }

    KeyValueStore<Object, Object> store = driver.getKeyValueStore(storeName);
    Collection<KeyValue<Object, Object>> unexpectedRecords = expectedRecordsFromMaps(storeConfig,
        unexpected, extraFieldsToDefault);

    Set<String> expectedFields = unexpected.iterator().next().keySet();

    expectedFields = AliasHelper.expandAliasKeys(expectedFields, storeConfig.getAliases());
    expectedFields
        .addAll(AliasHelper.expandAliasKeys(extraFieldsToDefault, storeConfig.getAliases()));
    List<KeyValue<Object, Object>> storeContentsStripped = new ArrayList<>();
    try (KeyValueIterator<Object, Object> iter = store.all()) {
      while (iter.hasNext()) {
        storeContentsStripped.add(stripUnnecessaryFields(iter.next(), expectedFields, storeConfig));
      }
    }

    for (KeyValue<Object, Object> unexpectedRecord : unexpectedRecords) {
      assertFalse(String.format("Store does not contain record with matching fields %s; values: %s",
          expectedFields.toString(), unexpectedRecord),
          storeContentsStripped.contains(unexpectedRecord));
    }
  }

  // TODO: raw types aliasing, conversions, defaults
  private <K, V> KeyValue<K, V> createRecordWithProcessing(Map<String, String> expectedEntryMap,
      TopicConfig<K, V> topicConfig) throws Exception {
    boolean keyIsBean = keyIsBean(topicConfig);
    boolean valueIsBean = valueIsBean(topicConfig);

    KeyValue<K, V> record = RecordBeanHelper
        .createRecord(typeConversionService, expectedEntryMap, topicConfig, keyIsBean, valueIsBean);

    return postProcessRecord(record);
  }

  private <V, K> KeyValue<K, V> postProcessRecord(KeyValue<K, V> record) throws Exception {
    K processedKey = null;
    V processedValue = null;
    for (Entry<Class<?>, RecordPostProcessor<?>> entry : postProcessors.entrySet()) {
      Class<?> forClass = entry.getKey();
      RecordPostProcessor<?> processor = entry.getValue();
      if (processedKey == null && forClass.isInstance(record.key)) {
        log.debug("Post-processing key {}", record.key);
        processedKey = useProcessor((RecordPostProcessor<K>) processor, record.key);
        log.debug("Processed key: {}", processedKey);
      }
      if (processedValue == null && forClass.isInstance(record.value)) {
        log.debug("Post-processing key {}", record.value);
        processedValue = useProcessor((RecordPostProcessor<V>) processor, record.value);
        log.debug("Processed value: {}", processedValue);
      }
      if (processedKey != null && processedValue != null) {
        break;
      }
    }
    processedKey = Optional.ofNullable(processedKey).orElse(record.key);
    processedValue = Optional.ofNullable(processedValue).orElse(record.value);

    return KeyValue.pair(processedKey, processedValue);
  }

  private <K> K useProcessor(RecordPostProcessor<K> processor, K key)
      throws Exception {
    return processor.process(key);
  }

  public <K, V> void assertOutputMap(Topic<K, V> topic,
      Collection<Map<String, String>> expectedRecords) throws Exception {
    assertOutputMap(topic, expectedRecords, Collections.emptySet());
  }

  public <K, V> void assertOutputMap(Topic<K, V> topic,
      Collection<Map<String, String>> expectedRecords, Set<String> extraFieldsToCompareWithDefaults)
      throws Exception {
    TopicConfig<K, V> topicConfig = outputTopicConfig(topic);

    List<KeyValue<K, V>> output = readOutput(topicConfig);
    if (output.isEmpty() && expectedRecords.isEmpty()) {
      // both empty, no need to do anything else
      return;
    }

    Map<K, V> outputMap = new HashMap<>();
    for (KeyValue<K, V> outputRecord : output) {
      outputMap.put(outputRecord.key, outputRecord.value);
    }

    List<KeyValue<K, V>> reducedOutput = new ArrayList<>();
    outputMap.forEach((key, value) -> reducedOutput.add(new KeyValue<>(key, value)));

    if (!output.isEmpty()) {
      assertFalse("output is not empty but expectedOutput is. At least 1 record is required "
          + "in 'expectedRecords' for in-depth comparison", expectedRecords.isEmpty());
    }

    Set<String> expectedFields = AliasHelper
        .expandAliasKeys(expectedRecords.iterator().next().keySet(), topicConfig.getAliases());
    expectedFields.addAll(
        AliasHelper.expandAliasKeys(extraFieldsToCompareWithDefaults, topicConfig.getAliases()));

    List<KeyValue<K, V>> comparableOutput = stripUnnecessaryFields(reducedOutput, expectedFields,
        topicConfig);

    Collection<KeyValue<K, V>> expectedOutput = expectedRecordsFromMaps(topicConfig,
        expectedRecords, extraFieldsToCompareWithDefaults);

    assertListEquals(expectedOutput, comparableOutput, true);
  }

  private <K, V> List<KeyValue<K, V>> stripUnnecessaryFields(List<KeyValue<K, V>> records,
      Set<String> necessaryFields, TopicConfig<K, V> topicConfig) throws Exception {

    // strippedRecords will contain the same records as input, but with only necessary fields populated
    List<KeyValue<K, V>> strippedRecords = new ArrayList<>();
    for (KeyValue<K, V> originalRecord : records) {
      strippedRecords.add(stripUnnecessaryFields(originalRecord, necessaryFields, topicConfig));
    }
    return strippedRecords;
  }

  private <K, V> KeyValue<K, V> stripUnnecessaryFields(KeyValue<K, V> record,
      Set<String> necessaryFields, TopicConfig<K, V> topicConfig) throws Exception {
    boolean keyIsBean = keyIsBean(topicConfig);
    boolean valueIsBean = valueIsBean(topicConfig);

    KeyValue<K, V> strippedRecord = RecordBeanHelper
        .copyRecordPropertiesIntoNew(necessaryFields, record, topicConfig, keyIsBean, valueIsBean);
    return postProcessRecord(strippedRecord);
  }

  private void assertListEquals(Collection<?> expected, Collection<?> actual,
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
    return config.getConfiguredOutput().readKeyValuesToList();
  }

  @SuppressWarnings("unchecked")
  private <K, V> TopicConfig<K, V> inputTopicConfig(Topic<K, V> topic) {
    TopicConfig<?, ?> topicConfig = inputTopicConfig.get(topic.getName());
    if (topicConfig == null) {
      throw new IllegalStateException(
          String.format("Input topic %s is not configured", topic.getName()));
    }
    return (TopicConfig<K, V>) topicConfig;
  }

  @SuppressWarnings("unchecked")
  private <K, V> TopicConfig<K, V> outputTopicConfig(Topic<K, V> topic) {
    TopicConfig<?, ?> topicConfig = outputTopicConfig.get(topic.getName());
    if (topicConfig == null) {
      throw new IllegalStateException(
          String.format("Output topic %s is not configured", topic.getName()));
    }
    return (TopicConfig<K, V>) topicConfig;
  }

  @SuppressWarnings("unchecked")
  private <K, V> TopicConfig<K, V> storeConfig(String storeName) {
    TopicConfig<?, ?> topicConfig = storeConfig.get(storeName);
    if (topicConfig == null) {
      throw new IllegalStateException(
          String.format("Store %s is not configured", storeName));
    }
    return (TopicConfig<K, V>) topicConfig;
  }

  @Override
  public void close() {
    driver.close();
  }
}
