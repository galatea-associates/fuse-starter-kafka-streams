package org.galatea.kafka.starter.testing.bean.editor;

import java.beans.PropertyEditorSupport;
import org.joda.time.LocalDate;

/**
 * Add support to BeanWrapper to allow conversion from string to Joda LocalDate.
 */
public class JodaLocalDateEditor extends PropertyEditorSupport {

  public void setAsText(String text) {
    setValue(LocalDate.parse(text));
  }
}
