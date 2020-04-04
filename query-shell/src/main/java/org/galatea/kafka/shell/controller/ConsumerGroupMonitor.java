package org.galatea.kafka.shell.controller;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.galatea.kafka.shell.domain.OffsetMap;
import org.galatea.kafka.shell.domain.TopicOffsetType;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ConsumerGroupMonitor {

  private final AdminClient adminClient;
  private final ConsumerThreadController consumerThreadController;

  public Map<String, OffsetMap> sumOffsetsByTopic(
      Map<TopicPartition, OffsetMap> partitionOffsets) {

    Map<String, OffsetMap> topicOffsets = new HashMap<>();
    partitionOffsets
        .forEach((topicPart, offsetMap) -> topicOffsets.put(topicPart.topic(), new OffsetMap()));

    partitionOffsets.forEach((partition, offsets) -> topicOffsets
        .computeIfAbsent(partition.topic(), k -> new OffsetMap()).add(offsets));

    return topicOffsets;
  }

  public Map<TopicPartition, OffsetMap> getPartitionOffsets(String groupName)
      throws InterruptedException, ExecutionException {
    Map<TopicPartition, OffsetAndMetadata> groupCommitOffsets = adminClient
        .listConsumerGroupOffsets(groupName).partitionsToOffsetAndMetadata().get();

    Set<String> topics = groupCommitOffsets.keySet().stream().map(TopicPartition::topic)
        .collect(Collectors.toSet());

    Map<TopicPartition, OffsetMap> allOffsets = new HashMap<>(
        consumerThreadController.getTopicOffsets(topics));

    for (Entry<TopicPartition, OffsetMap> entry : allOffsets.entrySet()) {
      OffsetAndMetadata commitDetails = groupCommitOffsets.get(entry.getKey());
      if (commitDetails != null) {
        entry.getValue().put(TopicOffsetType.COMMIT_OFFSET, commitDetails.offset());
      }
    }

    return allOffsets;
  }
}