/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by Grant Wade on 2021-01-15:
 *  - Add constructor argument for MockCluster
 *  - Multiple tasks allowed, number determined by MockCluster#numPartitions
 *  - Records assigned to tasks according to provided Partitioner or DefaultPartitioner if one is not provided
 *  - Removed methods that are not applicable with multiple tasks:
 *    - getStateStore(String)
 *    - getTimestampedKeyValueStore(String)
 *    - getWindowStore(String)
 *    - getTimestampedWindowStore(String)
 *    - getSessionStore(String)
 *  - Removed deprecated pipeInput(ConsumerRecord)
 *  - getKeyValueStore(String) returns facade store that aggregates underlying task stores (read-only)
 */
package org.apache.kafka.streams;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.internals.QuietStreamsConfig;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.GlobalProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.GlobalStateManager;
import org.apache.kafka.streams.processor.internals.GlobalStateManagerImpl;
import org.apache.kafka.streams.processor.internals.GlobalStateUpdateTask;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.processor.internals.StoreChangelogReader;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecordingTrigger;
import org.apache.kafka.streams.test.TestRecord;
import org.galatea.kafka.starter.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopologyTestDriver implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(TopologyTestDriver.class);

  private final Time mockWallClockTime;
  private final InternalTopologyBuilder internalTopologyBuilder;

  private final List<StreamTask> tasks = new ArrayList<>();
  private final GlobalStateUpdateTask globalStateTask;
  private final GlobalStateManager globalStateManager;

  private final StateDirectory stateDirectory;
  private final Metrics metrics;
  final ProcessorTopology processorTopology;
  final ProcessorTopology globalTopology;

  private final MockProducer<byte[], byte[]> producer;

  private final Set<String> internalTopics = new HashSet<>();
  private final Map<String, List<TopicPartition>> partitionsByInputTopic = new HashMap<>();
  private final Map<String, List<TopicPartition>> globalPartitionsByInputTopic = new HashMap<>();
  private final Map<TopicPartition, AtomicLong> offsetsByTopicOrPatternPartition = new HashMap<>();

  private final Map<String, Queue<ProducerRecord<byte[], byte[]>>> outputRecordsByTopic = new HashMap<>();
  private final boolean eosEnabled;
  private final Partitioner partitioner;
  private final MockCluster cluster;
  private final int partitionCount;
  private final Map<String, Pair<Deserializer<?>, Deserializer<?>>> topicDeserializers = new HashMap<>();

  /**
   * Create a new test diver instance. Initialized the internally mocked wall-clock time with {@link
   * System#currentTimeMillis() current system time}.
   *
   * @param topology the topology to be tested
   * @param config the configuration for the topology
   */
  public TopologyTestDriver(final Topology topology,
      final Properties config, MockCluster cluster) {
    this(topology, config, null, new DefaultPartitioner(), cluster);
  }

  /**
   * Create a new test diver instance.
   *
   * @param topology the topology to be tested
   * @param config the configuration for the topology
   * @param initialWallClockTime the initial value of internally mocked wall-clock time
   */
  @Builder
  public TopologyTestDriver(@NonNull final Topology topology,
      @NonNull final Properties config,
      final Instant initialWallClockTime,
      final Partitioner partitioner,
      @NonNull final MockCluster cluster) {
    this(
        topology.internalTopologyBuilder,
        config,
        initialWallClockTime == null ? System.currentTimeMillis()
            : initialWallClockTime.toEpochMilli(),
        partitioner != null ? partitioner : new DefaultPartitioner(),
        cluster);
  }

  /**
   * Create a new test diver instance.
   *
   * @param builder builder for the topology to be tested
   * @param config the configuration for the topology
   * @param initialWallClockTimeMs the initial value of internally mocked wall-clock time
   */
  private TopologyTestDriver(final InternalTopologyBuilder builder,
      final Properties config,
      final long initialWallClockTimeMs,
      Partitioner partitioner,
      MockCluster cluster) {
    partitionCount = cluster.getNumPartitions();
    this.cluster = cluster;
    this.partitioner = partitioner;
    final StreamsConfig streamsConfig = new QuietStreamsConfig(config);
    logIfTaskIdleEnabled(streamsConfig);
    mockWallClockTime = new MockTime(initialWallClockTimeMs);

    internalTopologyBuilder = builder;
    internalTopologyBuilder.rewriteTopology(streamsConfig);

    processorTopology = internalTopologyBuilder.build(null);
    globalTopology = internalTopologyBuilder.buildGlobalStateTopology();

    final boolean createStateDirectory = processorTopology.hasPersistentLocalStore() ||
        (globalTopology != null && globalTopology.hasPersistentGlobalStore());
    stateDirectory = new StateDirectory(streamsConfig, mockWallClockTime, createStateDirectory);

    final Serializer<byte[]> bytesSerializer = new ByteArraySerializer();
    producer = new MockProducer<>(cluster, true, partitioner, bytesSerializer,
        bytesSerializer);

    final MetricConfig metricConfig = new MetricConfig()
        .samples(streamsConfig.getInt(StreamsConfig.METRICS_NUM_SAMPLES_CONFIG))
        .recordLevel(Sensor.RecordingLevel
            .forName(streamsConfig.getString(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG)))
        .timeWindow(streamsConfig.getLong(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
            TimeUnit.MILLISECONDS);

    metrics = new Metrics(metricConfig, mockWallClockTime);

    final String threadId = Thread.currentThread().getName();
    final StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(
        metrics,
        "test-client",
        StreamsMetricsImpl.METRICS_LATEST
    );
    streamsMetrics.setRocksDBMetricsRecordingTrigger(new RocksDBMetricsRecordingTrigger());
    final Sensor skippedRecordsSensor =
        streamsMetrics.threadLevelSensor(threadId, "skipped-records", Sensor.RecordingLevel.INFO);
    final String threadLevelGroup = "stream-metrics";
    skippedRecordsSensor.add(new MetricName("skipped-records-rate",
            threadLevelGroup,
            "The average per-second number of skipped records",
            streamsMetrics.tagMap(threadId)),
        new Rate(TimeUnit.SECONDS, new WindowedCount()));
    skippedRecordsSensor.add(new MetricName("skipped-records-total",
            threadLevelGroup,
            "The total number of skipped records",
            streamsMetrics.tagMap(threadId)),
        new CumulativeSum());
    final ThreadCache cache = new ThreadCache(
        new LogContext("topology-test-driver "),
        Math.max(0, streamsConfig.getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG)),
        streamsMetrics);
    final StateRestoreListener stateRestoreListener = new StateRestoreListener() {
      @Override
      public void onRestoreStart(final TopicPartition topicPartition, final String storeName,
          final long startingOffset, final long endingOffset) {
      }

      @Override
      public void onBatchRestored(final TopicPartition topicPartition, final String storeName,
          final long batchEndOffset, final long numRestored) {
      }

      @Override
      public void onRestoreEnd(final TopicPartition topicPartition, final String storeName,
          final long totalRestored) {
      }
    };

    for (final InternalTopologyBuilder.TopicsInfo topicsInfo : internalTopologyBuilder.topicGroups()
        .values()) {
      internalTopics.addAll(topicsInfo.repartitionSourceTopics.keySet());
    }

    final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    for (final String topic : processorTopology.sourceTopics()) {
      List<TopicPartition> topicParts = cluster.availablePartitionsForTopic(topic)
          .stream().map(p -> new TopicPartition(p.topic(), p.partition()))
          .sorted(Comparator.comparingInt(TopicPartition::partition))
          .collect(Collectors.toList());
      partitionsByInputTopic.put(topic, topicParts);
      for (TopicPartition tp : topicParts) {
        offsetsByTopicOrPatternPartition.put(tp, new AtomicLong());
      }
    }

    consumer.assign(partitionsByInputTopic.values().stream().flatMap(List::stream)
        .collect(Collectors.toList()));

    if (globalTopology != null) {
      for (final String topicName : globalTopology.sourceTopics()) {
        List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topicName);
        List<TopicPartition> topicPartitions = partitions.stream()
            .map(p -> new TopicPartition(p.topic(), p.partition()))
            .collect(Collectors.toList());
        globalPartitionsByInputTopic.put(topicName, topicPartitions);
        for (TopicPartition tp : topicPartitions) {
          offsetsByTopicOrPatternPartition.put(tp, new AtomicLong());
        }

        consumer.updatePartitions(topicName, partitions);
        Map<TopicPartition, Long> updateOffsetMap = topicPartitions.stream()
            .collect(Collectors.toMap(p -> p, p -> 0L));
        consumer.updateBeginningOffsets(updateOffsetMap);
        consumer.updateEndOffsets(updateOffsetMap);
      }

      globalStateManager = new GlobalStateManagerImpl(
          new LogContext("mock "),
          globalTopology,
          consumer,
          stateDirectory,
          stateRestoreListener,
          streamsConfig);

      final GlobalProcessorContextImpl globalProcessorContext =
          new GlobalProcessorContextImpl(streamsConfig, globalStateManager, streamsMetrics,
              cache);
      globalStateManager.setGlobalProcessorContext(globalProcessorContext);

      globalStateTask = new GlobalStateUpdateTask(
          globalTopology,
          globalProcessorContext,
          globalStateManager,
          new LogAndContinueExceptionHandler(),
          new LogContext()
      );
      globalStateTask.initialize();
      globalProcessorContext.setRecordContext(new ProcessorRecordContext(
          0L,
          -1L,
          -1,
          ProcessorContextImpl.NONEXIST_TOPIC,
          new RecordHeaders()));
    } else {
      globalStateManager = null;
      globalStateTask = null;
    }

    if (!partitionsByInputTopic.isEmpty()) {
      for (int i = 0; i < partitionCount; i++) {
        int partId = i;

        // each task is assigned 1 partition per topic
        Set<TopicPartition> taskPartitions = partitionsByInputTopic.values().stream()
            .map(l -> l.get(partId)).collect(Collectors.toSet());

        ProcessorTopology topology = internalTopologyBuilder.build(null);
        StreamTask task = new StreamTask(
            new TaskId(0, i),
            taskPartitions,
            topology,
            consumer,
            new StoreChangelogReader(
                createRestoreConsumer(topology.storeToChangelogTopic()),
                Duration.ZERO,
                stateRestoreListener,
                new LogContext("topology-test-driver ")),
            streamsConfig,
            streamsMetrics,
            stateDirectory,
            cache,
            mockWallClockTime,
            () -> producer);
        tasks.add(task);
        task.initializeStateStores();
        task.initializeTopology();
        ((InternalProcessorContext) task.context()).setRecordContext(new ProcessorRecordContext(
            0L,
            -1L,
            -1,
            ProcessorContextImpl.NONEXIST_TOPIC,
            new RecordHeaders()));
      }
    }
    eosEnabled = streamsConfig.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)
        .equals(StreamsConfig.EXACTLY_ONCE);
  }

  private static void logIfTaskIdleEnabled(final StreamsConfig streamsConfig) {
    final Long taskIdleTime = streamsConfig.getLong(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
    if (taskIdleTime > 0) {
      log.info("Detected {} config in use with TopologyTestDriver (set to {}ms)." +
              " This means you might need to use TopologyTestDriver#advanceWallClockTime()" +
              " or enqueue records on all partitions to allow Steams to make progress." +
              " TopologyTestDriver will log a message each time it cannot process enqueued" +
              " records due to {}.",
          StreamsConfig.MAX_TASK_IDLE_MS_CONFIG,
          taskIdleTime,
          StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
    }
  }

  /**
   * Get read-only handle on global metrics registry.
   *
   * @return Map of all metrics.
   */
  public Map<MetricName, ? extends Metric> metrics() {
    return Collections.unmodifiableMap(metrics.metrics());
  }

  private void pipeRecord(final String topicName,
      final long timestamp,
      final byte[] key,
      final byte[] value,
      final Headers headers, int partition) {
    final TopicPartition inputTopicOrPatternPartition = getInputTopicOrPatternPartition(topicName,
        partition);
    final TopicPartition globalInputTopicPartition = getGlobalTopicOrPatternPartition(topicName,
        partition);

    if (inputTopicOrPatternPartition == null && globalInputTopicPartition == null) {
      throw new IllegalArgumentException("Unknown topic: " + topicName);
    }

    if (globalInputTopicPartition != null) {
      processGlobalRecord(globalInputTopicPartition, timestamp, key, value, headers);
    } else {
      enqueueTaskRecord(topicName, inputTopicOrPatternPartition, timestamp, key, value, headers);
      completeAllProcessableWork();
    }

  }

  private TopicPartition getGlobalTopicOrPatternPartition(String topicName, int partition) {
    List<TopicPartition> topicPartitions = globalPartitionsByInputTopic.get(topicName);
    if (topicPartitions != null && !topicPartitions.isEmpty()) {
      return topicPartitions.get(partition);
    }
    return null;
  }

  private void enqueueTaskRecord(final String inputTopic,
      final TopicPartition topicOrPatternPartition,
      final long timestamp,
      final byte[] key,
      final byte[] value,
      final Headers headers) {

    if (!offsetsByTopicOrPatternPartition.containsKey(topicOrPatternPartition)) {
      // this record must not be consumed by stream tasks
      return;
    }
    StreamTask task = tasks.get(topicOrPatternPartition.partition());
    task.addRecords(topicOrPatternPartition, Collections.singleton(new ConsumerRecord<>(
        inputTopic,
        topicOrPatternPartition.partition(),
        offsetsByTopicOrPatternPartition.get(topicOrPatternPartition).incrementAndGet() - 1,
        timestamp,
        TimestampType.CREATE_TIME,
        (long) ConsumerRecord.NULL_CHECKSUM,
        key == null ? ConsumerRecord.NULL_SIZE : key.length,
        value == null ? ConsumerRecord.NULL_SIZE : value.length,
        key,
        value,
        headers)));
  }

  private void completeAllProcessableWork() {
    // for internally triggered processing (like wall-clock punctuations),
    // we might have buffered some records to internal topics that need to
    // be piped back in to kick-start the processing loop. This is idempotent
    // and therefore harmless in the case where all we've done is enqueued an
    // input record from the user.

    boolean recordsToProcess;
    do {
      recordsToProcess = false;

// 2020-12-28 Update: iterate through all tasks as long as there are records to process,
//  since a record may be repartitioned into another task
      for (StreamTask task : tasks) {
        if (task.hasRecordsQueued()) {

          task.process();
          task.maybePunctuateStreamTime();
          task.commit();

          recordsToProcess = true;

          captureOutputsAndReEnqueueInternalResults();
        }
      }

    } while (recordsToProcess);

    captureOutputsAndReEnqueueInternalResults();
  }

  private void processGlobalRecord(final TopicPartition globalInputTopicPartition,
      final long timestamp,
      final byte[] key,
      final byte[] value,
      final Headers headers) {
    globalStateTask.update(new ConsumerRecord<>(
        globalInputTopicPartition.topic(),
        globalInputTopicPartition.partition(),
        offsetsByTopicOrPatternPartition.get(globalInputTopicPartition).getAndIncrement(),
        timestamp,
        TimestampType.CREATE_TIME,
        (long) ConsumerRecord.NULL_CHECKSUM,
        key == null ? ConsumerRecord.NULL_SIZE : key.length,
        value == null ? ConsumerRecord.NULL_SIZE : value.length,
        key,
        value,
        headers));
    globalStateTask.flushState();
  }

  private void validateSourceTopicNameRegexPattern(final String inputRecordTopic) {
    for (final String sourceTopicName : internalTopologyBuilder.sourceTopicNames()) {
      if (!sourceTopicName.equals(inputRecordTopic) && Pattern.compile(sourceTopicName)
          .matcher(inputRecordTopic).matches()) {
        throw new TopologyException(
            "Topology add source of type String for topic: " + sourceTopicName +
                " cannot contain regex pattern for input record topic: " + inputRecordTopic +
                " and hence cannot process the message.");
      }
    }
  }

// TODO: rename this to not include "pattern"
  private TopicPartition getInputTopicOrPatternPartition(final String topicName, int partition) {
    if (!internalTopologyBuilder.sourceTopicNames().isEmpty()) {
      validateSourceTopicNameRegexPattern(topicName);
    }

// 2020-12-28 Update: No longer supports input topic patterns here, since
//  MockCluster "creates" any topics that are requested.
    List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topicName);
    if (!partitions.isEmpty()) {
      return new TopicPartition(topicName, partition);
    }
    return null;
  }

  private void captureOutputsAndReEnqueueInternalResults() {
    // Capture all the records sent to the producer ...
    final List<ProducerRecord<byte[], byte[]>> output = producer.history();
    producer.clear();
    if (eosEnabled && !producer.closed()) {
      producer.initTransactions();
      producer.beginTransaction();
    }
    for (final ProducerRecord<byte[], byte[]> record : output) {
      outputRecordsByTopic.computeIfAbsent(record.topic(), k -> new LinkedList<>()).add(record);

      // Forward back into the topology if the produced record is to an internal or a source topic ...
      final String outputTopicName = record.topic();

// 2020-12-28 Update: use record partition to get specific TopicPartition
      final TopicPartition inputTopicOrPatternPartition = getInputTopicOrPatternPartition(
          outputTopicName, record.partition());
      final TopicPartition globalInputTopicPartition = getGlobalTopicOrPatternPartition(
          outputTopicName, record.partition());

      if (inputTopicOrPatternPartition != null) {
        enqueueTaskRecord(
            outputTopicName,
            inputTopicOrPatternPartition,
            record.timestamp(),
            record.key(),
            record.value(),
            record.headers());
      }

      if (globalInputTopicPartition != null) {
        processGlobalRecord(
            globalInputTopicPartition,
            record.timestamp(),
            record.key(),
            record.value(),
            record.headers());
      }
    }
  }

// TODO: remove
  /**
   * {@link MockProducer#history()} doesn't record the assigned partition for "produced" records, so
   * need to use the provided {@link Partitioner} directly here
   */
  private int determinePartition(ProducerRecord<byte[], byte[]> record) {
    Pair<Deserializer<?>, Deserializer<?>> deserializers = topicDeserializers.get(record.topic());

    Object originalKey = deserializers.getKey().deserialize(record.topic(), record.key());
    Object originalValue = deserializers.getValue().deserialize(record.topic(), record.value());

    // make sure the topic is registered in the cluster before getting an immutable representation
    // of the cluster for the Partitioner
    cluster.createTopic(record.topic());
    return partitioner
        .partition(record.topic(), originalKey, record.key(), originalValue, record.value(),
            cluster.getImmutable());
  }

  /**
   * Advances the internally mocked wall-clock time. This might trigger a {@link
   * PunctuationType#WALL_CLOCK_TIME wall-clock} type {@link ProcessorContext#schedule(Duration,
   * PunctuationType, Punctuator) punctuations}.
   *
   * @param advanceMs the amount of time to advance wall-clock time in milliseconds
   * @deprecated Since 2.4 use {@link #advanceWallClockTime(Duration)} instead
   */
  @Deprecated
  public void advanceWallClockTime(final long advanceMs) {
    advanceWallClockTime(Duration.ofMillis(advanceMs));
  }

  /**
   * Advances the internally mocked wall-clock time. This might trigger a {@link
   * PunctuationType#WALL_CLOCK_TIME wall-clock} type {@link ProcessorContext#schedule(Duration,
   * PunctuationType, Punctuator) punctuations}.
   *
   * @param advance the amount of time to advance wall-clock time
   */
  public void advanceWallClockTime(final Duration advance) {
    Objects.requireNonNull(advance, "advance cannot be null");
    mockWallClockTime.sleep(advance.toMillis());
    // 2020-12-28 Update: update each stream task time
    for (StreamTask task : tasks) {
      if (task != null) {
        task.maybePunctuateSystemTime();
        task.commit();
      }
      completeAllProcessableWork();
    }
  }

  /**
   * Read the next record from the given topic. These records were output by the topology during the
   * previous calls to
   *
   * @param topic the name of the topic
   * @return the next record on that topic, or {@code null} if there is no record available
   * @deprecated Since 2.4 use methods of {@link TestOutputTopic} instead
   */
  @Deprecated
  public ProducerRecord<byte[], byte[]> readOutput(final String topic) {
    final Queue<ProducerRecord<byte[], byte[]>> outputRecords = outputRecordsByTopic.get(topic);
    if (outputRecords == null) {
      return null;
    }
    return outputRecords.poll();
  }

  /**
   * Read the next record from the given topic. These records were output by the topology during the
   * previous calls to
   *
   * @param topic the name of the topic
   * @param keyDeserializer the deserializer for the key type
   * @param valueDeserializer the deserializer for the value type
   * @return the next record on that topic, or {@code null} if there is no record available
   * @deprecated Since 2.4 use methods of {@link TestOutputTopic} instead
   */
  @Deprecated
  public <K, V> ProducerRecord<K, V> readOutput(final String topic,
      final Deserializer<K> keyDeserializer,
      final Deserializer<V> valueDeserializer) {
    final ProducerRecord<byte[], byte[]> record = readOutput(topic);
    if (record == null) {
      return null;
    }
    final K key = keyDeserializer.deserialize(record.topic(), record.key());
    final V value = valueDeserializer.deserialize(record.topic(), record.value());
    return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), key,
        value, record.headers());
  }

  private Queue<ProducerRecord<byte[], byte[]>> getRecordsQueue(final String topicName) {
    final Queue<ProducerRecord<byte[], byte[]>> outputRecords = outputRecordsByTopic
        .get(topicName);
    if (outputRecords == null) {
      if (!processorTopology.sinkTopics().contains(topicName)) {
        throw new IllegalArgumentException("Unknown topic: " + topicName);
      }
    }
    return outputRecords;
  }

  /**
   * Create {@link TestInputTopic} to be used for piping records to topic Uses current
   * system time as start timestamp for records. Auto-advance is disabled.
   *
   * @param topicName the name of the topic
   * @param keySerializer the Serializer for the key type
   * @param valueSerializer the Serializer for the value type
   * @param <K> the key type
   * @param <V> the value type
   * @return {@link TestInputTopic} object
   */
   // 2020-12-28 Update: use PartitionedTestInputTopic instead of TestInputTopic
   //  because of the new class PartitionedTopologyTestDriver
  public final <K, V> TestInputTopic<K, V> createInputTopic(final String topicName,
      final Serializer<K> keySerializer,
      final Serializer<V> valueSerializer) {
    return new TestInputTopic<>(this, topicName, keySerializer,
        valueSerializer, Instant.now(), Duration.ZERO);
  }

  /**
   * Create {@link TestOutputTopic} to be used for reading records from topic
   *
   * @param topicName the name of the topic
   * @param keyDeserializer the Deserializer for the key type
   * @param valueDeserializer the Deserializer for the value type
   * @param <K> the key type
   * @param <V> the value type
   * @return {@link TestOutputTopic} object
   */
   // 2020-12-28 Update: use PartitionedTestOutputTopic instead of TestOutputTopic
   //  because of the new class PartitionedTopologyTestDriver
  public final <K, V> TestOutputTopic<K, V> createOutputTopic(final String topicName,
      final Deserializer<K> keyDeserializer,
      final Deserializer<V> valueDeserializer) {
    TestOutputTopic<K, V> topic = new TestOutputTopic<>(this, topicName,
        keyDeserializer, valueDeserializer);
    topicDeserializers.put(topicName, Pair.of(keyDeserializer, valueDeserializer));
    return topic;
  }

  ProducerRecord<byte[], byte[]> readRecord(final String topic) {
    final Queue<? extends ProducerRecord<byte[], byte[]>> outputRecords = getRecordsQueue(topic);
    if (outputRecords == null) {
      return null;
    }
    return outputRecords.poll();
  }

  <K, V> TestRecord<K, V> readRecord(final String topic,
      final Deserializer<K> keyDeserializer,
      final Deserializer<V> valueDeserializer) {
    final Queue<? extends ProducerRecord<byte[], byte[]>> outputRecords = getRecordsQueue(topic);
    if (outputRecords == null) {
      throw new NoSuchElementException("Uninitialized topic: " + topic);
    }
    final ProducerRecord<byte[], byte[]> record = outputRecords.poll();
    if (record == null) {
      throw new NoSuchElementException("Empty topic: " + topic);
    }
    final K key = keyDeserializer.deserialize(record.topic(), record.key());
    final V value = valueDeserializer.deserialize(record.topic(), record.value());
    return new TestRecord<>(key, value, record.headers(), record.timestamp());
  }

  <K, V> void pipeRecord(final String topic,
      final TestRecord<K, V> record,
      final Serializer<K> keySerializer,
      final Serializer<V> valueSerializer,
      final Instant time) {
    final byte[] serializedKey = keySerializer.serialize(topic, record.headers(), record.key());
    final byte[] serializedValue = valueSerializer
        .serialize(topic, record.headers(), record.value());
    final long timestamp;
    if (time != null) {
      timestamp = time.toEpochMilli();
    } else if (record.timestamp() != null) {
      timestamp = record.timestamp();
    } else {
      throw new IllegalStateException(
          "Provided `TestRecord` does not have a timestamp and no timestamp overwrite was provided via `time` parameter.");
    }

    // 2020-12-28 Update: create topic in cluster before getting an immutable
    //  cluster representation for the partitioner. Determine partition and pass
    //  into pipeRecord so it can be assigned to the correct task
    cluster.createTopic(topic);
    int partition = partitioner
        .partition(topic, record.key(), serializedKey, record.value(), serializedValue,
            cluster.getImmutable());

    pipeRecord(topic, timestamp, serializedKey, serializedValue, record.headers(), partition);
  }

  final long getQueueSize(final String topic) {
    final Queue<ProducerRecord<byte[], byte[]>> queue = getRecordsQueue(topic);
    if (queue == null) {
      //Return 0 if not initialized, getRecordsQueue throw exception if non existing topic
      return 0;
    }
    return queue.size();
  }

  final boolean isEmpty(final String topic) {
    return getQueueSize(topic) == 0;
  }

  /**
   * Get all {@link StateStore StateStores} from the topology. The stores can be a "regular" or
   * global stores.
   * <p>
   * This is often useful in test cases to pre-populate the store before the test case instructs the
   * topology to process an input message}, and/or to check the store afterward.
   * <p>
   * Note, that {@code StateStore} might be {@code null} if a store is added but not connected to
   * any processor.
   * <p>
   * <strong>Caution:</strong> Using this method to access stores that are added by the DSL is
   * unsafe as the store types may change. Stores added by the DSL should only be accessed via the
   * corresponding typed methods like {@link #getKeyValueStore(String)} etc.
   *
   * @return all stores my name
   * @see #getKeyValueStore(String)
   */
   // 2020-12-28 Update: return collection of stores for each name, one per Stream Task
  public Map<String, Collection<StateStore>> getAllStateStores() {

    final Map<String, Collection<StateStore>> allStores = new HashMap<>();
    for (final String storeName : internalTopologyBuilder.allStateStoreName()) {

      Collection<StateStore> taskStores = new ArrayList<>(tasks.size());
      for (StreamTask task : tasks) {
        taskStores.add(getStateStore(storeName, false, task));
      }
      allStores.put(storeName, taskStores);
    }
    return allStores;
  }

  // 2020-12-28 Update: remove method StateStore getStateStore(name) because with multiple
  //  tasks you cannot get a single mutable store

// 2020-12-28 Update:  add StreamTask argument in order to retrieve a single store based on name
  private StateStore getStateStore(final String name,
      final boolean throwForBuiltInStores, StreamTask task) {

    if (task != null) {
      final StateStore stateStore = ((ProcessorContextImpl) task.context()).getStateMgr()
          .getStore(name);
      if (stateStore != null) {
        if (throwForBuiltInStores) {
          throwIfBuiltInStore(stateStore);
        }
        return stateStore;
      }
    }

    if (globalStateManager != null) {
      final StateStore stateStore = globalStateManager.getGlobalStore(name);
      if (stateStore != null) {
        if (throwForBuiltInStores) {
          throwIfBuiltInStore(stateStore);
        }
        return stateStore;
      }

    }

    return null;
  }

// 2020-12-28 Update: add method, in order to get all state stores for a given name
  private Collection<StateStore> getStateStores(final String name,
      final boolean throwForBuiltInStores) {

    return tasks.stream()
        .map(task -> getStateStore(name, throwForBuiltInStores, task))
        .collect(Collectors.toList());
  }

  private void throwIfBuiltInStore(final StateStore stateStore) {
    if (stateStore instanceof TimestampedKeyValueStore) {
      throw new IllegalArgumentException("Store " + stateStore.name()
          + " is a timestamped key-value store and should be accessed via `getTimestampedKeyValueStore()`");
    }
    if (stateStore instanceof ReadOnlyKeyValueStore) {
      throw new IllegalArgumentException("Store " + stateStore.name()
          + " is a key-value store and should be accessed via `getKeyValueStore()`");
    }
    if (stateStore instanceof TimestampedWindowStore) {
      throw new IllegalArgumentException("Store " + stateStore.name()
          + " is a timestamped window store and should be accessed via `getTimestampedWindowStore()`");
    }
    if (stateStore instanceof ReadOnlyWindowStore) {
      throw new IllegalArgumentException("Store " + stateStore.name()
          + " is a window store and should be accessed via `getWindowStore()`");
    }
    if (stateStore instanceof ReadOnlySessionStore) {
      throw new IllegalArgumentException("Store " + stateStore.name()
          + " is a session store and should be accessed via `getSessionStore()`");
    }
  }

  /**
   * Get the {@link KeyValueStore} or {@link TimestampedKeyValueStore} with the given name. The
   * store can be a "regular" or global store.
   * <p>
   * If the registered store is a {@link TimestampedKeyValueStore} this method will return a
   * value-only query interface. <strong>It is highly recommended to update the code for this case
   * to avoid bugs and to use for full store access instead.</strong>
   * <p>
   * This is often useful in test cases to pre-populate the store before the test case instructs the
   * topology to process an input message, and/or to check the store afterward.
   *
   * @param name the name of the store
   * @return the key value store, or {@code null} if no {@link KeyValueStore} or {@link
   * TimestampedKeyValueStore} has been registered with the given name
   */
   // 2020-12-28 Update: make the returned store a read-only store that is an
   //  aggregate store of all the underlying task stores
  @SuppressWarnings("unchecked")
  public <K, V> KeyValueStore<K, V> getKeyValueStore(final String name) {
    Collection<StateStore> stores = getStateStores(name, false);

    Collection<KeyValueStore<K, V>> toBeWrapped = new ArrayList<>(stores.size());
    for (StateStore store : stores) {
      toBeWrapped.add((KeyValueStore<K, V>) store);
    }
    AggregateKeyValueStore<K, V> aggStore = new AggregateKeyValueStore<>(toBeWrapped);

    if (!stores.isEmpty()
        && stores.stream().findFirst().orElse(null) instanceof TimestampedKeyValueStore) {
      throw new IllegalStateException(
          "Method #getTimestampedKeyValueStore() should be used to access a TimestampedKeyValueStore.");

    }
    return aggStore;
  }

// 2020-12-28 Update: remove methods getTimestampedKeyValueStore,
//  getWindowStore, getTimestampedWindowStore, getSessionStore

  /**
   * Close the driver, its topology, and all processors.
   */
  public void close() {
  // 2020-12-28 Update: close each stream task
    for (StreamTask task : tasks) {
      if (task != null) {
        task.close(true, false);
      }
    }
    if (globalStateTask != null) {
      try {
        globalStateTask.close();
      } catch (final IOException e) {
        // ignore
      }
    }
    completeAllProcessableWork();
    for (StreamTask task : tasks) {
      if (task != null && task.hasRecordsQueued()) {
        log.warn("Found some records that cannot be processed due to the" +
                " {} configuration during TopologyTestDriver#close().",
            StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
      }
    }
    if (!eosEnabled) {
      producer.close();
    }
    stateDirectory.clean();
  }

  static class MockTime implements Time {

    private final AtomicLong timeMs;
    private final AtomicLong highResTimeNs;

    MockTime(final long startTimestampMs) {
      this.timeMs = new AtomicLong(startTimestampMs);
      this.highResTimeNs = new AtomicLong(startTimestampMs * 1000L * 1000L);
    }

    @Override
    public long milliseconds() {
      return timeMs.get();
    }

    @Override
    public long nanoseconds() {
      return highResTimeNs.get();
    }

    @Override
    public long hiResClockMs() {
      return TimeUnit.NANOSECONDS.toMillis(nanoseconds());
    }

    @Override
    public void sleep(final long ms) {
      if (ms < 0) {
        throw new IllegalArgumentException("Sleep ms cannot be negative.");
      }
      timeMs.addAndGet(ms);
      highResTimeNs.addAndGet(TimeUnit.MILLISECONDS.toNanos(ms));
    }

    @Override
    public void waitObject(final Object obj, final Supplier<Boolean> condition,
        final long timeoutMs) {
      throw new UnsupportedOperationException();
    }

  }

  private MockConsumer<byte[], byte[]> createRestoreConsumer(
      final Map<String, String> storeToChangelogTopic) {
    final MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(
        OffsetResetStrategy.LATEST) {
      @Override
      public synchronized void seekToEnd(final Collection<TopicPartition> partitions) {
      }

      @Override
      public synchronized void seekToBeginning(final Collection<TopicPartition> partitions) {
      }

      @Override
      public synchronized long position(final TopicPartition partition) {
        return 0L;
      }
    };

    // for each store
    for (final Map.Entry<String, String> storeAndTopic : storeToChangelogTopic.entrySet()) {
      final String topicName = storeAndTopic.getValue();
      // Set up the restore-state topic ...
      // consumer.subscribe(new TopicPartition(topicName, 0));
      // Set up the partition that matches the ID (which is what ProcessorStateManager expects) ...
      final List<PartitionInfo> partitionInfos = new ArrayList<>();

      // 2020-12-28 Update:  update consumer with all partitions
      for (int partId = 0; partId < partitionCount; partId++) {
        partitionInfos.add(new PartitionInfo(topicName, partId, null, null, null));
        consumer.updatePartitions(topicName, partitionInfos);
        consumer.updateEndOffsets(
            Collections.singletonMap(new TopicPartition(topicName, partId), 0L));

      }
    }
    return consumer;
  }
}
