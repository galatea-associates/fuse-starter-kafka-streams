package org.galatea.kafka.shell.consumer.request;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.galatea.kafka.shell.domain.TopicPartitionOffsets;

public class ConsumerOffsetRequest extends
    ConsumerRequest<Map<TopicPartition, TopicPartitionOffsets>> {

  @Override
  public Map<TopicPartition, TopicPartitionOffsets> fulfillRequest(
      Consumer<byte[], byte[]> consumer) {
    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
    Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(consumer.assignment());
    Map<TopicPartition, TopicPartitionOffsets> offsets = new HashMap<>();
    endOffsets.forEach(((topicPartition, endOffset) -> {
      Long beginningOffset = beginningOffsets.get(topicPartition);
      offsets.put(topicPartition, new TopicPartitionOffsets(beginningOffset, endOffset));
    }));
    return offsets;
  }
}
