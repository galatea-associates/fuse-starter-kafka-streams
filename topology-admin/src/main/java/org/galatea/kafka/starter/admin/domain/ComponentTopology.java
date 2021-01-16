package org.galatea.kafka.starter.admin.domain;

import java.util.Collection;
import lombok.Value;

@Value
public class ComponentTopology {

  String component;
  Collection<StateNode> stateNodes;
  String topologyUml;
}
