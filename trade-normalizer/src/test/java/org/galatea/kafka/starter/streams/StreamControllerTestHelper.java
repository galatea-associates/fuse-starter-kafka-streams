package org.galatea.kafka.starter.streams;

import org.apache.kafka.streams.Topology;

public class StreamControllerTestHelper {

  public static Topology buildTopology(StreamController controller) {
    return controller.buildTopology();
  }
}
