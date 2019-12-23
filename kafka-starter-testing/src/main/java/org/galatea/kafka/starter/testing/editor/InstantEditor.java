package org.galatea.kafka.starter.testing.editor;

import java.beans.PropertyEditorSupport;
import java.time.Instant;

public class InstantEditor extends PropertyEditorSupport {

  public void setAsText(String text) {
    setValue(Instant.parse(text));
  }
}
