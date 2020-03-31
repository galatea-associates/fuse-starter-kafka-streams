package org.galatea.kafka.shell.consumer.request;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

@RequiredArgsConstructor
public class TopicOffsetRequest extends ConsumerRequest<Map<TopicPartition, Long>> {

  private final String topicName;

  @Override
  Map<TopicPartition, Long> fulfillRequest(Consumer<byte[], byte[]> consumer) {

    List<TopicPartition> partitions = consumer.partitionsFor(topicName).stream()
        .map(partInfo -> new TopicPartition(partInfo.topic(), partInfo.partition())).collect(
            Collectors.toList());
    return consumer.endOffsets(partitions);
  }
}
