package org.galatea.kafka.starter.util;

public interface Translator<I, O> {

  O apply(I input);
}
