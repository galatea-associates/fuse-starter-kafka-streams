package org.galatea.kafka.starter.admin.service;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.galatea.kafka.starter.admin.config.NodeTypeConfig;
import org.galatea.kafka.starter.admin.domain.ComponentTopology;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TopologyRegistrar {

  private final Map<String, ComponentTopology> registeredTopologies = new HashMap<>();
  private final NodeTypeConfig nodeTypeConfig;
  private static final String UML_PREFIX = "@startuml\n"
      + "skinparam componentStyle rectangle\n\n";
  private static final String UML_SUFFIX = "\n\n@enduml";

  public void registerTopology(ComponentTopology topology) {
    registeredTopologies.put(topology.getComponent(), topology);
  }

  public String getFullTopology() {

    StringBuilder legendEntries = new StringBuilder();
    nodeTypeConfig.getColors().forEach((type, color) -> legendEntries
        .append(String.format("| <back:%s>   </back> | %s |\n", color, type)));
    nodeTypeConfig.getReadTypeArrow().forEach((type, format) -> legendEntries
        .append(String.format("| %s | READ %s |\n", format, type)));

    String legend = "legend\n"
        + "|= |= Type |\n"
        + legendEntries.toString()
        + "endlegend\n";

    String fullTopology = registeredTopologies.values().stream()
        .map(ComponentTopology::getTopologyUml)
        .collect(Collectors.joining("\n\n", UML_PREFIX, legend + UML_SUFFIX));
    log.info("Full topology: \n{}", fullTopology);
    return fullTopology;
  }
}
