package org.galatea.kafka.starter.testing;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.unitils.reflectionassert.ReflectionAssert.assertReflectionEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.testing.alias.AliasHelper;
import org.unitils.reflectionassert.ReflectionComparatorMode;

@RequiredArgsConstructor
public class OutputAssertionBuilder<K, V> {

  private final Topic<K, V> topic;
  private final TopologyTester tester;
  private Collection<Map<String, String>> expectedRows;
  private OutputAssertionMode mode = OutputAssertionMode.LIST;
  private boolean orderedComparison = true;
  private final Set<String> explicitlyVerifyFields = new HashSet<>();

  public OutputAssertionBuilder<K, V> withExpectedRows(Collection<Map<String, String>> rows) {
    this.expectedRows = rows;
    return this;
  }

  public OutputAssertionBuilder<K, V> usingMode(OutputAssertionMode mode) {
    this.mode = mode;
    if (mode == OutputAssertionMode.MAP) {
      orderedComparison = false;
    }
    return this;
  }

  public OutputAssertionBuilder<K, V> withFieldsToVerifyDefault(
      Set<String> explicitlyVerifyFields) {
    this.explicitlyVerifyFields.clear();
    this.explicitlyVerifyFields.addAll(explicitlyVerifyFields);
    return this;
  }

  public OutputAssertionBuilder<K, V> useOrderedComparison(boolean orderedComparison) {
    this.orderedComparison = orderedComparison;
    return this;
  }

  public void doAssert() {
    TopicConfig<K, V> topicConfig = tester.outputTopicConfig(topic);

    List<KeyValue<K, V>> output = tester.readOutput(topicConfig);
    if (output.isEmpty() && expectedRows.isEmpty()) {
      // both empty, no need to do anything else
      return;
    }

    if (mode == OutputAssertionMode.MAP) {
      // reduce list to latest value per key
      output = output.stream()
          .collect(Collectors.toMap(kv -> kv.key, kv -> kv.value))
          .entrySet().stream()
          .map(e -> KeyValue.pair(e.getKey(), e.getValue()))
          .collect(Collectors.toList());
    }

    if (!output.isEmpty()) {
      assertFalse("output is not empty but expectedOutput is. At least 1 record is required "
          + "in 'expectedRecords' for in-depth comparison", expectedRows.isEmpty());
    }

    Set<String> expectedFields = AliasHelper
        .expandAliasKeys(expectedRows.iterator().next().keySet(), topicConfig.getAliases());
    expectedFields.addAll(
        AliasHelper.expandAliasKeys(explicitlyVerifyFields, topicConfig.getAliases()));

    List<KeyValue<K, V>> comparableOutput = tester.stripUnnecessaryFields(output, expectedFields,
        topicConfig);

    Collection<KeyValue<K, V>> expectedOutput = tester.expectedRecordsFromMaps(topicConfig,
        expectedRows, explicitlyVerifyFields);

    if (mode == OutputAssertionMode.MAP) {
      // reduce list to latest value per key
      expectedOutput = expectedOutput.stream()
          .collect(Collectors.toMap(kv -> kv.key, kv -> kv.value))
          .entrySet().stream()
          .map(e -> KeyValue.pair(e.getKey(), e.getValue()))
          .collect(Collectors.toList());
    }

    boolean lenientOrder = mode == OutputAssertionMode.MAP || !orderedComparison;
    assertListEquals(expectedOutput, comparableOutput, lenientOrder);
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

      StringBuilder sb = new StringBuilder("Expected and Actual lists do not match:");
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

}
