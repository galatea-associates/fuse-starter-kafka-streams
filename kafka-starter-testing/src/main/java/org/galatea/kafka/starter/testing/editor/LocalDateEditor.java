package org.galatea.kafka.starter.testing.editor;

import java.beans.PropertyEditorSupport;
import java.time.LocalDate;

public class LocalDateEditor extends PropertyEditorSupport {

  public void setAsText(String text) {
    setValue(LocalDate.parse(text));
  }
}
