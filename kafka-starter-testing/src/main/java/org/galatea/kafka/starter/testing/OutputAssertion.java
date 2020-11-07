package org.galatea.kafka.starter.testing;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.kafka.streams.KeyValue;
import org.galatea.kafka.starter.messaging.Topic;

@Getter
@Builder
public class OutputAssertion<K, V> {

  private final Topic<K, V> outputTopic;
  @NonNull
  private final List<Map<String, String>> expectedRecords;
  /**
   * These fields will always be asserted. If the field does not exist in the maps in {@link
   * OutputAssertion#expectedRecords}, then it will be checked against the default value for the
   * field.
   */
  @Builder.Default
  private final Set<String> alwaysAssertFields = new HashSet<>();
  /**
   * Not relevant if {@link OutputAssertion#flattenToLatestValuePerKey} is true
   */
  private final boolean checkRecordOrder;
  /**
   * Only compare the last value that was output for each key
   */
  private final boolean flattenToLatestValuePerKey;
  @Builder.Default
  private final Function<KeyValue<K, V>, KeyValue<K, V>> recordCreationCallback = kv -> kv;

  public static <K, V> OutputAssertionBuilder<K, V> builder(Topic<K, V> topic) {
    return new OutputAssertionBuilder<>(topic);
  }

  public static class OutputAssertionBuilder<K, V> {

    private Topic<K, V> outputTopic;

    private OutputAssertionBuilder() {
      throw new UnsupportedOperationException();
    }

    OutputAssertionBuilder(Topic<K, V> topic) {
      outputTopic = topic;
    }
  }
}
