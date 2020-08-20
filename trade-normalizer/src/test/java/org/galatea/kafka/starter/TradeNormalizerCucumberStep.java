package org.galatea.kafka.starter;

import io.cucumber.core.api.Scenario;
import io.cucumber.datatable.DataTable;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import java.time.Duration;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.Topology;
import org.galatea.kafka.starter.messaging.KafkaStreamsConfig;
import org.galatea.kafka.starter.messaging.KafkaStreamsStarter;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.security.SecurityIsinMsgKey;
import org.galatea.kafka.starter.messaging.security.SecurityMsgValue;
import org.galatea.kafka.starter.messaging.streams.GStreamBuilder;
import org.galatea.kafka.starter.messaging.trade.TradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.TradeMsgValue;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgValue;
import org.galatea.kafka.starter.streams.StreamController;
import org.galatea.kafka.starter.testing.TopologyTester;
import org.galatea.kafka.starter.testing.avro.AvroPostProcessor;
import org.junit.Ignore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@Slf4j
@SpringBootTest
@EnableAutoConfiguration
@Ignore   // Without this, IntelliJ will try to run this class, find no tests, and error
public class TradeNormalizerCucumberStep {

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
  @MockBean
  private KafkaStreamsStarter mockStreamsStarter;   // mock KafkaStreamsStarter, so the real bean won't try to start KafkaStreams

  private static TopologyTester tester;

  @Before
  public void setup(Scenario scenario) {

    if (tester == null) {
      Topology topology = controller.buildTopology(new GStreamBuilder());
      tester = new TopologyTester(topology, properties.asProperties());
      tester.configureInputTopic(securityTopic, SecurityIsinMsgKey::new, SecurityMsgValue::new);
      tester.configureInputTopic(inputTradeTopic, InputTradeMsgKey::new, InputTradeMsgValue::new);
      tester.configureOutputTopic(normalizedTradeTopic, TradeMsgKey::new, TradeMsgValue::new);

      tester.registerBeanClass(SpecificRecord.class);
      tester.registerPostProcessor(SpecificRecord.class, AvroPostProcessor.defaultUtil());
    }
    tester.beforeTest();
    log.info("Running scenario: {}", scenario.getName());
  }

  @SneakyThrows
  @Given("^receive the following security records:$")
  public void securityTable(DataTable table) {
    tester.pipeInput(securityTopic, table.asMaps());
  }

  @SneakyThrows
  @Given("^receive the following trade records:$")
  public void tradeTable(DataTable table) {
    tester.pipeInput(inputTradeTopic, table.asMaps());
  }

  @SneakyThrows
  @Then("^output the following trade records:$")
  public void outputTable(DataTable table) {
    tester.assertOutputList(normalizedTradeTopic, table.asMaps(), true);
  }

  @Given("time passes")
  public void timePasses() {
    tester.getDriver().advanceWallClockTime(Duration.ofDays(120));
  }
}
