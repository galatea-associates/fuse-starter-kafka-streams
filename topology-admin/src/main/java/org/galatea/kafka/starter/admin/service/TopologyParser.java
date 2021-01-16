package org.galatea.kafka.starter.admin.service;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.galatea.kafka.starter.admin.config.NodeTypeConfig;
import org.galatea.kafka.starter.admin.domain.AccessLevel;
import org.galatea.kafka.starter.admin.domain.RegisterTopologyRequest;
import org.galatea.kafka.starter.admin.domain.StateNode;
import org.galatea.kafka.starter.admin.util.NStringBuilder;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TopologyParser {

  private static final String RIGHT_ARROW = "->";
  private static final String BOTH_ARROW = "<->";
  private static final String READ_ARROW = "<-";
  private final NodeTypeConfig nodeTypeConfig;

  public String parse(RegisterTopologyRequest request) {

    Collection<StateNode> nodes = request.getStateNodes();
    Collection<StateNode> privateNodes = new LinkedList<>();
    Collection<StateNode> publicReadNodes = new LinkedList<>();
    Collection<StateNode> publicWriteNodes = new LinkedList<>();

    for (StateNode node : nodes) {
      if (node.getAccess() == AccessLevel.PRIVATE) {
        privateNodes.add(node);
      } else if (node.isReads()) {
        publicReadNodes.add(node);
      } else if (node.isWrites()) {
        publicWriteNodes.add(node);
      }
    }

    NStringBuilder sb = new NStringBuilder();
    UmlProcessDetails umlProcessDetails = processUmlNode(request.getComponent(), privateNodes);
    String processNodeName = umlProcessDetails.getProcessNodeName();

    sb.append(umlProcessDetails.getUml());

    //[service-1-process] <-> [topic-<topic-name>]
    for (StateNode publicNode : publicReadNodes) {
      String nodeFullName = "topic-" + publicNode.getName();
//      String arrow = getArrow(publicNode.isReads(), publicNode.isWrites(), reversed);
      sb.append("[%s] as \"%s\" %s\n", nodeFullName, publicNode.getName(),
          nodeColor(publicNode.getType()));
      sb.append(connectProcessToNode(processNodeName, nodeFullName, publicNode));
//      sb.append("[%s] %s [%s]\n", processNodeName, arrow, nodeFullName);
    }

    //[service-1-process] <-> [topic-<topic-name>]
    for (StateNode publicNode : publicWriteNodes) {
      String nodeFullName = "topic-" + publicNode.getName();
//      String arrow = getArrow(publicNode.isReads(), publicNode.isWrites(), reversed);
      sb.append("[%s] as \"%s\" %s\n", nodeFullName, publicNode.getName(),
          nodeColor(publicNode.getType()));
//      sb.append("[%s] %s [%s]\n", processNodeName, arrow, nodeFullName);
      sb.append(connectProcessToNode(processNodeName, nodeFullName, publicNode));
    }

    String uml = sb.toString();
    log.info("Parsed UML:\n{}", uml);
    return uml;
  }

  private String connectProcessToNode(String processNodeName, String nodeFullName, StateNode node) {
    String readType = node.getReadType();
    String lineType = nodeTypeConfig.getReadTypeArrow().getOrDefault(readType, "-");
    if (node.isReads() && !node.isWrites()) {
      // node -> process
      return String.format("[%s] %s> [%s]\n", nodeFullName, lineType, processNodeName);
    } else if (node.isReads() && node.isWrites()) {
      // process <-> node
      return String.format("[%s] <%s> [%s]\n", processNodeName, lineType, nodeFullName);
    } else if (!node.isReads() && node.isWrites()) {
      // process -> node
      return String.format("[%s] %s> [%s]\n", processNodeName, lineType, nodeFullName);
    }
    return "";
  }

  private String nodeColor(String nodeType) {
    String colorConfig = nodeTypeConfig.getColors().get(nodeType);
    return Optional.ofNullable(colorConfig).map(s -> "#" + s).orElse("");
  }

  private UmlProcessDetails processUmlNode(String componentName,
      Collection<StateNode> privateNodes) {
    NStringBuilder sb = new NStringBuilder();
    String processNodeName;

    if (privateNodes.isEmpty()) {
      processNodeName = componentName;
      sb.append("[%s]\n", componentName);
    } else {
      sb.append("component %s {\n", componentName);

      //   [service-1-process] as "process"
      processNodeName = String.format("%s-process", componentName);
      sb.append("  [%s] as \"process\"\n", processNodeName);
      for (StateNode privateNode : privateNodes) {
        //   [service-1-process] as "process"
        String nodeFullName = componentName + "-" + privateNode.getName();
        sb.append("  [%s] as \"%s\" %s\n", nodeFullName, privateNode.getName(),
            nodeColor(privateNode.getType()));
      }
      sb.append("}\n");

      //[service-1-process] <-> [service-1-internal-topic]
      for (StateNode privateNode : privateNodes) {
        String nodeFullName = componentName + "-" + privateNode.getName();
//        String arrow = getArrow(privateNode.isReads(), privateNode.isWrites(), reversed);
//        sb.append("[%s] %s [%s]\n", processNodeName, arrow, nodeFullName);
        sb.append(connectProcessToNode(processNodeName, nodeFullName, privateNode));
      }
    }

    return new UmlProcessDetails(processNodeName, sb.toString());
  }

  @Value
  private static class UmlProcessDetails {

    String processNodeName;
    String uml;
  }

  private String getArrow(boolean isRead, boolean isWrite, boolean reversed) {
    if (isRead && isWrite) {
      return BOTH_ARROW;
    } else if (!isWrite) {
      return !reversed ? READ_ARROW : RIGHT_ARROW;
    } else {
      return !reversed ? RIGHT_ARROW : READ_ARROW;
    }
  }
}
