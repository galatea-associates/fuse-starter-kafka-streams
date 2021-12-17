package org.galatea.kafka.starter.diagram;

import lombok.Value;

@Value
public class Participant {

  String name;
  UmlBuilder builder;

  Participant(String name, UmlBuilder builder) {
    this.name = name;
    this.builder = builder;
  }

  public String getName() {
    return name;
  }

  public void to(Participant toParticipant, String msg) {
    builder.addArrow(this, toParticipant, msg);
  }

  public void to(Participant toParticipant, KvPair kv) {
    builder.addArrow(this, toParticipant, kv.toString());
  }

  public void to(Participant toParticipant) {
    to(toParticipant, (String) null);
  }


  public void chain(KvPair pair, Participant... participants) {
    Participant src = this;
    for (Participant participant : participants) {
      src.to(participant, pair);
      src = participant;
    }
  }

  public void chain(String msg, Participant... participants) {
    Participant src = this;
    for (Participant participant : participants) {
      src.to(participant, msg);
      src = participant;
    }
  }

  public void note(String text) {
    builder.addNote(this, text);
  }
}
