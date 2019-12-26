package org.galatea.kafka.starter.testing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.SneakyThrows;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.galatea.kafka.starter.messaging.Topic;
import org.junit.Before;
import org.junit.Test;

public class TopologyTesterTest {

  private TopologyTester tester;
  private static final Topology testTopology = TestTopology.topology();
  private static final Topic<String, String> inputTopic = TestTopology.inputTopic;
  private static final Topic<String, String> outputTopic = TestTopology.outputTopic;
  private static final String storeName = TestTopology.STORE_NAME;
  private static final Properties streamProperties;

  static {
    streamProperties = new Properties();
    streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app.id");
    streamProperties.put(StreamsConfig.STATE_DIR_CONFIG, "/target/testStateDir");
    streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:65535");
  }

  @Before
  public void setup() {
    if (tester == null) {
      tester = new TopologyTester(testTopology, streamProperties);

      tester.configureInputTopic(inputTopic, String::new, String::new);
      tester.configureOutputTopic(outputTopic, String::new, String::new);
      tester.configureStore(storeName, Serdes.String(), Serdes.String(), String::new, String::new);

      tester.registerAvroClass(SpecificRecord.class);
      tester.registerBeanClass(SpecificRecord.class);
    }
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
    tester.pipeInput(inputTopic, inputRecord);

    // then
    tester.assertOutputList(outputTopic, expected, false);
  }
}