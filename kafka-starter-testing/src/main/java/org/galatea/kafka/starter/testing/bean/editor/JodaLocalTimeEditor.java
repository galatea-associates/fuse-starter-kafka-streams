package org.galatea.kafka.starter.testing.bean.editor;

import java.beans.PropertyEditorSupport;
import org.joda.time.LocalTime;

/**
 * Add support to BeanWrapper to allow conversion from string to Joda LocalTime.
 */
public class JodaLocalTimeEditor extends PropertyEditorSupport {

  public void setAsText(String text) {
    setValue(LocalTime.parse(text));
  }
}
