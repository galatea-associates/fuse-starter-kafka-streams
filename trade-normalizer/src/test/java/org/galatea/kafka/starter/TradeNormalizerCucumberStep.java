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
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.config.KafkaConfig;
import org.galatea.kafka.starter.messaging.security.SecurityIsinMsgKey;
import org.galatea.kafka.starter.messaging.security.SecurityMsgValue;
import org.galatea.kafka.starter.messaging.trade.TradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.TradeMsgValue;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgValue;
import org.galatea.kafka.starter.testing.OutputAssertion;
import org.galatea.kafka.starter.testing.TopologyTester;
import org.galatea.kafka.starter.testing.avro.AvroPostProcessor;
import org.junit.Ignore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@EnableAutoConfiguration
@ActiveProfiles("test")
@Ignore   // Without this, IntelliJ will try to run this class, find no tests, and error
public class TradeNormalizerCucumberStep {

  @Autowired
  private KafkaConfig config;
  @Autowired
  private Topic<InputTradeMsgKey, InputTradeMsgValue> inputTradeTopic;
  @Autowired
  private Topic<SecurityIsinMsgKey, SecurityMsgValue> securityTopic;
  @Autowired
  private Topic<TradeMsgKey, TradeMsgValue> normalizedTradeTopic;
  @Autowired
  private Topology topology;

  private TopologyTester tester;

  @Before
  public void setup(Scenario scenario) {

    tester = new TopologyTester(topology, config.streamsProperties());
    tester.configureInputTopic(securityTopic, SecurityIsinMsgKey::new, SecurityMsgValue::new);
    tester.configureInputTopic(inputTradeTopic, InputTradeMsgKey::new, InputTradeMsgValue::new);
    tester.configureOutputTopic(normalizedTradeTopic, TradeMsgKey::new, TradeMsgValue::new);

    tester.registerBeanClass(SpecificRecord.class);
    tester.registerPostProcessor(SpecificRecord.class, AvroPostProcessor.defaultUtil());

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
    tester.assertOutput(OutputAssertion.builder(normalizedTradeTopic)
        .expectedRecords(table.asMaps())
        .checkRecordOrder(false)
        .build());
  }

  @Given("time passes")
  public void timePasses() {
    tester.getDriver().advanceWallClockTime(Duration.ofDays(120));
  }
}
