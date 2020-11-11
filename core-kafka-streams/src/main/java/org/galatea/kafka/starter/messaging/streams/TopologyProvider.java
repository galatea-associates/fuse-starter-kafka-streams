package org.galatea.kafka.starter.messaging.streams;

import org.apache.kafka.streams.Topology;

public interface TopologyProvider {

  Topology buildTopology(GStreamBuilder builder);
}
