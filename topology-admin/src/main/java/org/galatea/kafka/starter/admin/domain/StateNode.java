package org.galatea.kafka.starter.admin.domain;

import lombok.Data;

@Data
public class StateNode {

  String name;
  String type;
  AccessLevel access;
  boolean writes;
  boolean reads;
  String readType;
  String writeType;
}
