package org.galatea.kafka.starter.diagram;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class UmlBuilder {
  private final Set<String> participants = new LinkedHashSet<>();

  private final List<String> instructions = new LinkedList<>();

  public Participant participant(String name) {
    participants.add(name);
    return new Participant(name, this);
  }

  public State state(Participant participant) {
    return new State(participant, this);
  }

  public void addNote(Participant over, String text) {
    addNote(over, text, false);
  }

  public void addNote(Participant over, String text, boolean inlineWithLastNode) {
    StringBuilder sb = new StringBuilder();
    if (inlineWithLastNode) {
      sb.append("/ ");
    }
    sb.append(String.format("note over %s\n%s\nend note", over.getName(), text));
    instructions.add(sb.toString());
  }

  public String build() {
    String participantString = participants.stream().map(p -> String.format("participant %s", p)).collect(Collectors.joining("\n"));
    String instructionString = String.join("\n", instructions);
    return "@startuml\n" +
        "!theme cerulean\n" +
        "skinparam backgroundColor Mintcream\n\n" +
        participantString + "\n\n" +
        instructionString + "\n\n" +
        "@enduml";
  }

  public void addArrow(Participant from, Participant to, String msg) {
    String msgString = Optional.ofNullable(msg).map(m -> ": " + m).orElse("");
    instructions.add(String.format("%s -> %s%s", from.getName(), to.getName(), msgString));
  }
}
