package org.galatea.kafka.starter.testing.bean.editor;

import java.beans.PropertyEditorSupport;
import org.joda.time.DateTime;

/**
 * Add support to BeanWrapper to allow conversion from string to Joda DateTime.
 */
public class JodaDateTimeEditor extends PropertyEditorSupport {

  public void setAsText(String text) {
    setValue(DateTime.parse(text));
  }
}
