package org.apache.kafka.streams;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;

@Data
@Accessors(fluent = true)
public class MockClusterBuilder {

  private String clusterId;
  private final Collection<Node> nodes = new HashSet<>();
  private final Collection<PartitionInfo> partitions = new HashSet<>();
  private final Set<String> unauthorizedTopics = new HashSet<>();
  private final Set<String> invalidTopics = new HashSet<>();
  private final Set<String> internalTopics = new HashSet<>();
  private final Node controller;

  private int defaultPartitionCount = 1;

  public MockClusterBuilder() {
    controller = new Node(1, "mock-host", 1092);
    nodes.add(controller);
  }

  public MockClusterBuilder withDefaultPartitions(int defaultValue) {
    if (!partitions.isEmpty()) {
      throw new IllegalStateException("withDefaultPartitions cannot be called after adding topics");
    }
    defaultPartitionCount = defaultValue;
    return this;
  }

  public MockClusterBuilder withTopic(String name) {
    Node[] nodes = this.nodes.toArray(new Node[0]);
    for (int i = 0; i < defaultPartitionCount; i++) {
      partitions.add(new PartitionInfo(name, i, controller, nodes, nodes));
    }
    return this;
  }

  public Cluster build() {
    String clusterId = this.clusterId;
    if (clusterId == null) {
      clusterId = "mock-cluster";
    }

    return new Cluster(clusterId, nodes, partitions, unauthorizedTopics, invalidTopics,
        internalTopics, controller);
  }
}
