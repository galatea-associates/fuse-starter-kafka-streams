package org.galatea.kafka.starter.testing.editor;

import java.beans.PropertyEditorSupport;
import java.time.LocalTime;

/**
 * Add support to BeanWrapper to allow conversion from string to LocalTime.
 */
public class LocalTimeEditor extends PropertyEditorSupport {

  public void setAsText(String text) {
    setValue(LocalTime.parse(text));
  }
}
