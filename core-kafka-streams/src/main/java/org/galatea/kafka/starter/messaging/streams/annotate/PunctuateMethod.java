package org.galatea.kafka.starter.messaging.streams.annotate;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface PunctuateMethod {

  /**
   * parseable duration for intervals between punctuate calls.<br> see {@link
   * java.time.Duration#parse(CharSequence)}
   */
  String value();

}
