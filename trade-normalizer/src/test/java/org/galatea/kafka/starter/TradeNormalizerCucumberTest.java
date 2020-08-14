package org.galatea.kafka.starter;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
    features = {
        "src/test/resources/cucumber/securityNormalization.feature",
    },
    strict = true,
    plugin = {"pretty"})
public class TradeNormalizerCucumberTest {

}
