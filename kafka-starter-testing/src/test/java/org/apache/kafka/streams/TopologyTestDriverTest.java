package org.apache.kafka.streams;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class TopologyTestDriverTest {

  private static final String INPUT_TOPIC = "input-topic";
  private static final String STORE_NAME = "store-name";
  private static final String OUTPUT_TOPIC = "output-topic";
  private static final String REPARTITION_TOPIC = "repartition-topic";
  private TestOutputTopic<String, String> testOutputTopic;
  private TestInputTopic<String, String> testInputTopic;
  private UnaryOperator<String> mapNextKey;

  @Before
  public void setup() {
    // disable key mapping, a test will define this if it's needed for test
    mapNextKey = null;
  }

  private void setupTopology(boolean repartitionAfterMap) {
    StreamsBuilder builder = new StreamsBuilder();

    builder.addStateStore(Stores
        .keyValueStoreBuilder(Stores.inMemoryKeyValueStore(STORE_NAME), Serdes.String(),
            Serdes.String()));

    KStream<String, String> stream = builder
        .stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
        .map((KeyValueMapper<String, String, KeyValue<String, String>>) (key, value) -> KeyValue
            .pair(mapNextKey != null ? mapNextKey.apply(key) : key, value));

    if (repartitionAfterMap) {
      stream = stream.through(REPARTITION_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

    stream
        .transform(() -> new Transformer<String, String, KeyValue<String, String>>() {

          private ProcessorContext context;
          private KeyValueStore<String, String> store;

          @Override
          public void init(ProcessorContext context) {
            this.context = context;
            store = (KeyValueStore<String, String>) context.getStateStore(STORE_NAME);
          }

          @Override
          public KeyValue<String, String> transform(String key, String value) {
            log.info("Task {} processing {} | {}", context.taskId(), key, value);
            AtomicLong counter = new AtomicLong(0);
            store.put(key, value);
            store.all().forEachRemaining(kv -> counter.incrementAndGet());
            return KeyValue.pair(key, String.valueOf(counter.get()));
          }

          @Override
          public void close() {

          }
        }, STORE_NAME)
        .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:65535");

    TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config,
        new MockCluster(10));
    Serde<String> serde = Serdes.String();
    testInputTopic = driver.createInputTopic(INPUT_TOPIC, serde.serializer(), serde.serializer());
    testOutputTopic = driver
        .createOutputTopic(OUTPUT_TOPIC, serde.deserializer(), serde.deserializer());
  }

  @Test
  public void testTasksHaveSegregatedState() {
    setupTopology(false);

    // keys "0", "1", "2" are assigned to separate partitions, so will be processed by separate
    // tasks
    testInputTopic.pipeInput("0", "value");
    List<KeyValue<String, String>> output = testOutputTopic.readKeyValuesToList();
    assertEquals(1, output.size());
    assertEquals("1", output.get(0).value);

    testInputTopic.pipeInput("1", "value");
    output = testOutputTopic.readKeyValuesToList();
    assertEquals(1, output.size());
    assertEquals("1", output.get(0).value);

    testInputTopic.pipeInput("2", "value");
    output = testOutputTopic.readKeyValuesToList();
    assertEquals(1, output.size());
    assertEquals("1", output.get(0).value);
  }

  @Test
  public void recordsOnSameTaskShareState() {
    setupTopology(false);

    // "3" and "4" are both assigned to task 3 (with 10 partitions), so after the 2nd record the
    // state store will have 2 entries
    testInputTopic.pipeInput("3", "value");
    List<KeyValue<String, String>> output = testOutputTopic.readKeyValuesToList();
    assertEquals(1, output.size());
    assertEquals("1", output.get(0).value);

    testInputTopic.pipeInput("4", "value");
    output = testOutputTopic.readKeyValuesToList();
    assertEquals(1, output.size());
    assertEquals("2", output.get(0).value);
  }

  @Test
  public void recordsAreNotImmediatelyReassignedTaskOnMap() {
    setupTopology(false);

    // keys "0", "1", "2" are assigned to separate partitions, so will be processed by separate
    // tasks
    mapNextKey = k -> "0";
    testInputTopic.pipeInput("0", "value");
    List<KeyValue<String, String>> output = testOutputTopic.readKeyValuesToList();
    assertEquals(1, output.size());
    assertEquals("1", output.get(0).value);

    // this record will be consumed by task associated with key "1", immediately mapped to "0".
    // It should not move to the task associated with "0"
    testInputTopic.pipeInput("1", "value");
    output = testOutputTopic.readKeyValuesToList();
    assertEquals(1, output.size());
    assertEquals("1", output.get(0).value);

    // this record will be consumed by task associated with key "1", immediately mapped to "0".
    // It should not move to the task associated with "0"
    testInputTopic.pipeInput("2", "value");
    output = testOutputTopic.readKeyValuesToList();
    assertEquals(1, output.size());
    assertEquals("1", output.get(0).value);
  }

  @Test
  public void recordsAreReassignedTaskOnRepartition() {
    setupTopology(true);

    // This record will be consumed by task for "0", mapped to key "3", repartitioned, consumed by task for "3"
    mapNextKey = k -> "3";
    testInputTopic.pipeInput("0", "value");
    List<KeyValue<String, String>> output = testOutputTopic.readKeyValuesToList();
    assertEquals(1, output.size());
    assertEquals("1", output.get(0).value);

    // this record will be consumed by task for "1", mapped to key "4", repartitioned, consumed by task for "4"
    // since "3" and "4" are put on the same partition, they will be processed by the same task so
    // there will be 2 entries in the store after the 2nd record
    mapNextKey = k -> "4";
    testInputTopic.pipeInput("1", "value");
    output = testOutputTopic.readKeyValuesToList();
    assertEquals(1, output.size());
    assertEquals("2", output.get(0).value);
  }

}