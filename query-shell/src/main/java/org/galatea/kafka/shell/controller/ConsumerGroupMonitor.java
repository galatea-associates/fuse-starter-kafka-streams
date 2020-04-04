package org.galatea.kafka.shell.controller;

import static org.galatea.kafka.shell.domain.TopicOffsetType.COMMIT_OFFSET;
import static org.galatea.kafka.shell.domain.TopicOffsetType.COMMIT_OFFSET_DELTA_PER_SECOND;

import java.time.Instant;
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
import org.galatea.kafka.starter.util.Pair;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ConsumerGroupMonitor {

  private final Map<TopicPartition, Pair<Instant, OffsetMap>> latestResults = new HashMap<>();
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
        entry.getValue().put(COMMIT_OFFSET, commitDetails.offset());
      }
    }
    Instant now = Instant.now();
    allOffsets.forEach(((topicPartition, offsetMap) -> {
      Pair<Instant, OffsetMap> lastResults = latestResults.get(topicPartition);
      if (lastResults != null && lastResults.getKey().isAfter(now.minusSeconds(30))) {

        long commitOffsetDelta =
            offsetMap.get(COMMIT_OFFSET) - lastResults.getValue().get(COMMIT_OFFSET);
        double secondsSinceLast =
            ((double) now.toEpochMilli() - lastResults.getKey().toEpochMilli()) / 1000;
        long commitOffsetRate = Math.round(commitOffsetDelta / secondsSinceLast);
        offsetMap.put(COMMIT_OFFSET_DELTA_PER_SECOND, commitOffsetRate);
      }
      latestResults.put(topicPartition, new Pair<>(now, offsetMap));
    }));

    return allOffsets;
  }
}
