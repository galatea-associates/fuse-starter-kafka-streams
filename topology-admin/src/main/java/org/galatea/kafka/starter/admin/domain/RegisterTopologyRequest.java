package org.galatea.kafka.starter.admin.domain;

import java.util.Collection;
import lombok.Data;

@Data
public class RegisterTopologyRequest {

  Collection<StateNode> stateNodes;
  String component;
}
