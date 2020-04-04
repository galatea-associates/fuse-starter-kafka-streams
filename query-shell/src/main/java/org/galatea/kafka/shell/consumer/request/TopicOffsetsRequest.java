package org.galatea.kafka.shell.consumer.request;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.galatea.kafka.shell.domain.OffsetMap;
import org.galatea.kafka.shell.domain.TopicOffsetType;

@RequiredArgsConstructor
public class TopicOffsetsRequest extends
    ConsumerRequest<Map<TopicPartition, OffsetMap>> {

  private final Collection<String> topics;

  @Override
  Map<TopicPartition, OffsetMap> fulfillRequest(Consumer<byte[], byte[]> consumer) {

    List<TopicPartition> partitions = new LinkedList<>();
    topics.forEach(topic -> partitions.addAll(consumer.partitionsFor(topic)
        .stream()
        .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
        .collect(Collectors.toList())));

    Map<TopicPartition, OffsetMap> outputMap = new HashMap<>();
    Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);

    consumer.endOffsets(partitions).forEach(((topicPartition, endOffset) -> {
      OffsetMap offsets = outputMap
          .computeIfAbsent(topicPartition, key -> new OffsetMap());
      offsets.put(TopicOffsetType.END_OFFSET, endOffset);
      offsets.put(TopicOffsetType.BEGIN_OFFSET, beginningOffsets.get(topicPartition));
    }));

    return outputMap;
  }
}
