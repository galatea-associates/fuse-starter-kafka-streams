package org.galatea.kafka.shell.consumer.request;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.galatea.kafka.shell.domain.OffsetMap;
import org.galatea.kafka.shell.domain.TopicOffsetType;

public class ConsumerTopicOffsetsRequest extends
    ConsumerRequest<Map<TopicPartition, OffsetMap>> {

  @Override
  public Map<TopicPartition, OffsetMap> fulfillRequest(
      Consumer<byte[], byte[]> consumer) {

    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
    Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(consumer.assignment());
    Map<TopicPartition, OffsetMap> offsets = new HashMap<>();
    endOffsets.forEach(((topicPartition, endOffset) -> {

      Long beginningOffset = beginningOffsets.get(topicPartition);
      OffsetMap partOffsets = offsets
          .computeIfAbsent(topicPartition, key -> new OffsetMap());
      partOffsets.put(TopicOffsetType.BEGIN_OFFSET, beginningOffset);
      partOffsets.put(TopicOffsetType.END_OFFSET, endOffset);
    }));
    return offsets;
  }
}
