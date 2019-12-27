package org.galatea.kafka.starter.testing.bean.editor;

import java.beans.PropertyEditorSupport;
import java.time.LocalDate;

/**
 * Add support to BeanWrapper to allow conversion from string to LocalDate.
 */
public class LocalDateEditor extends PropertyEditorSupport {

  public void setAsText(String text) {
    setValue(LocalDate.parse(text));
  }
}
