package org.apache.kafka.streams;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class MockCluster {

  private final int numPartitions;
  private final String clusterId;
  private final Collection<Node> nodes = new HashSet<>();
  private final Map<String, List<PartitionInfo>> partitionsByTopic = new HashMap<>();
  private final Set<String> unauthorizedTopics = new HashSet<>();
  private final Set<String> invalidTopics = new HashSet<>();
  private final Set<String> internalTopics = new HashSet<>();
  private final Node controller;

  public MockCluster(int numPartitions) {
    clusterId = "mock-cluster";
    controller = new Node(1, "mock-host", 1092);

    nodes.add(controller);
    this.numPartitions = numPartitions;
  }

  public static MockCluster empty() {
    return new MockCluster(1);
  }

  public MockCluster createTopic(String name) {
    if (partitionsByTopic.containsKey(name)) {
      return this;
    }
    Node[] nodes = {controller};
    List<PartitionInfo> parts = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      parts.add(new PartitionInfo(name, i, controller, nodes, nodes));
    }
    partitionsByTopic.put(name, parts);
    return this;
  }

  public MockCluster createUnauthorizedTopic(String name) {
    unauthorizedTopics.add(name);
    return this;
  }

  public MockCluster createInvalidTopic(String name) {
    invalidTopics.add(name);
    return this;
  }

  public MockCluster createInternalTopic(String name) {
    internalTopics.add(name);
    return this;
  }

  public Cluster getImmutable() {
    Collection<PartitionInfo> partitions = partitionsByTopic.values().stream().flatMap(List::stream)
        .collect(Collectors.toList());
    return new Cluster(clusterId, nodes, partitions, unauthorizedTopics, invalidTopics,
        internalTopics, controller);
  }

  public List<PartitionInfo> availablePartitionsForTopic(String topic) {
    createTopic(topic);
    return new ArrayList<>(partitionsByTopic.getOrDefault(topic, Collections.emptyList()));
  }

  private TopicPartition convertPartitionInfo(PartitionInfo p) {
    return new TopicPartition(p.topic(), p.partition());
  }

  public List<PartitionInfo> partitionsForTopic(String topic) {

    return availablePartitionsForTopic(topic);
  }
}
