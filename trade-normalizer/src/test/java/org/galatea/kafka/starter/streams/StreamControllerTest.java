package org.galatea.kafka.starter.streams;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.galatea.kafka.starter.TestConfig;
import org.galatea.kafka.starter.messaging.StreamProperties;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.security.SecurityIsinMsgKey;
import org.galatea.kafka.starter.messaging.security.SecurityMsgValue;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgValue;
import org.galatea.kafka.starter.testing.TopicConfig;
import org.galatea.kafka.starter.testing.TopologyTester;
import org.galatea.kafka.starter.testing.bean.RecordBeanHelper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@Slf4j
@SpringBootTest
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {TestConfig.class, StreamController.class})
@EnableAutoConfiguration
public class StreamControllerTest {

  @Autowired
  private StreamController controller;
  @Autowired
  private StreamProperties properties;
  @Autowired
  private Topic<InputTradeMsgKey, InputTradeMsgValue> inputTradeTopic;
  @Autowired
  private Topic<SecurityIsinMsgKey, SecurityMsgValue> securityTopic;

  private static TopologyTestDriver driver = null;
  private static ConsumerRecordFactory<InputTradeMsgKey, InputTradeMsgValue> tradeFactory;
  private static ConsumerRecordFactory<SecurityIsinMsgKey, SecurityMsgValue> securityFactory;
  private static TopologyTester tester;

  @Before
  public void setup() {
    driver = new TopologyTestDriver(controller.buildTopology(), properties.asProperties());
    tradeFactory = new ConsumerRecordFactory<>(inputTradeTopic.getName(),
        inputTradeTopic.getKeySerde().serializer(), inputTradeTopic.getValueSerde().serializer());
    securityFactory = new ConsumerRecordFactory<>(securityTopic.getName(),
        securityTopic.getKeySerde().serializer(), securityTopic.getValueSerde().serializer());
    tester = new TopologyTester();
  }

  @Test
  @SneakyThrows
  public void testInput() {
    TopicConfig<SecurityIsinMsgKey, SecurityMsgValue> securityTopicConfig = new TopicConfig<>(
        SecurityIsinMsgKey::new, SecurityMsgValue::new);
    Map<String, String> securityRecordMap = new HashMap<>();
    securityRecordMap.put("isin", "isin1");
    securityRecordMap.put("securityId", "secId");
    KeyValue<SecurityIsinMsgKey, SecurityMsgValue> securityRecord = RecordBeanHelper
        .createRecord(securityRecordMap, securityTopicConfig);

    Map<String, String> tradeRecordMap = new HashMap<>();
    tradeRecordMap.put("isin", "isin1");
    TopicConfig<InputTradeMsgKey, InputTradeMsgValue> tradeTopicConfig = new TopicConfig<>(
        InputTradeMsgKey::new, InputTradeMsgValue::new);
    KeyValue<InputTradeMsgKey, InputTradeMsgValue> tradeRecord = RecordBeanHelper
        .createRecord(tradeRecordMap, tradeTopicConfig);

    List<ConsumerRecord<byte[], byte[]>> securityRecordRaw = securityFactory
        .create(Collections.singletonList(securityRecord));

    log.info("Piping in record: {}", securityRecord);
    log.info("Piping in record: {} | {}", securityRecordRaw.get(0).key(), securityRecordRaw.get(0).value());
    driver.pipeInput(securityRecordRaw);
    driver.pipeInput(tradeFactory.create(Collections.singletonList(tradeRecord)));
  }
}