package org.galatea.kafka.starter.streams;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.galatea.kafka.starter.TestConfig;
import org.galatea.kafka.starter.messaging.KafkaStreamsConfig;
import org.galatea.kafka.starter.messaging.KafkaStreamsStarter;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.security.SecurityIsinMsgKey;
import org.galatea.kafka.starter.messaging.security.SecurityMsgValue;
import org.galatea.kafka.starter.messaging.trade.TradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.TradeMsgValue;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgValue;
import org.galatea.kafka.starter.testing.TopologyTester;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@Slf4j
@SpringBootTest
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {TestConfig.class, StreamController.class})
@EnableAutoConfiguration
public class StreamControllerTest {

  @MockBean
  private KafkaStreamsStarter streamsStarter;
  @Autowired
  private StreamController controller;
  @Autowired
  private KafkaStreamsConfig properties;
  @Autowired
  private Topic<InputTradeMsgKey, InputTradeMsgValue> inputTradeTopic;
  @Autowired
  private Topic<SecurityIsinMsgKey, SecurityMsgValue> securityTopic;
  @Autowired
  private Topic<TradeMsgKey, TradeMsgValue> normalizedTradeTopic;

  private static TopologyTester tester;

  @Before
  public void setup() {

    if (tester == null) {
      tester = new TopologyTester(controller.buildTopology(), properties.asProperties());
      tester.configureInputTopic(securityTopic, SecurityIsinMsgKey::new, SecurityMsgValue::new);
      tester.configureInputTopic(inputTradeTopic, InputTradeMsgKey::new, InputTradeMsgValue::new);
      tester.configureOutputTopic(normalizedTradeTopic, TradeMsgKey::new, TradeMsgValue::new);

      tester.registerBeanClass(SpecificRecord.class);
      tester.registerAvroClass(SpecificRecord.class);
    }
    tester.beforeTest();

  }

  @Test
  @SneakyThrows
  public void testInput() {

    Runtime.getRuntime().traceMethodCalls(true);
    Runtime.getRuntime().traceInstructions(true);
    Map<String, String> securityRecordMap = new HashMap<>();
    securityRecordMap.put("isin", "isin1");
    securityRecordMap.put("securityId", "secId");
    tester.pipeInput(securityTopic, securityRecordMap);

    Map<String, String> tradeRecordMap = new HashMap<>();
    tradeRecordMap.put("isin", "isin1");
    tradeRecordMap.put("qty", "10");
    tester.pipeInput(inputTradeTopic, tradeRecordMap);

    Map<String, String> expectedOutput = new HashMap<>();
    expectedOutput.put("securityId", "secId");
    tester.assertOutputList(normalizedTradeTopic, Collections.singletonList(expectedOutput), true);
  }

  @Test
  @SneakyThrows
  public void testInput2() {

    Map<String, String> securityRecordMap = new HashMap<>();
    securityRecordMap.put("isin", "isin2");
    securityRecordMap.put("securityId", "secId");
    tester.pipeInput(securityTopic, securityRecordMap);

    Map<String, String> tradeRecordMap = new HashMap<>();
    tradeRecordMap.put("isin", "isin1");
    tester.pipeInput(inputTradeTopic, tradeRecordMap);
  }

}