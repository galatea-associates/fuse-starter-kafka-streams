package org.galatea.kafka.starter.testing.editor;

import java.beans.PropertyEditorSupport;
import java.time.Instant;

/**
 * Add support to BeanWrapper to allow conversion from string to Instant.
 */
public class InstantEditor extends PropertyEditorSupport {

  public void setAsText(String text) {
    setValue(Instant.parse(text));
  }
}
