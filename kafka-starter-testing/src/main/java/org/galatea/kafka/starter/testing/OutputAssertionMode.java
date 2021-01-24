package org.galatea.kafka.starter.testing;

public enum OutputAssertionMode {
  /**
   * Before comparing output, use key for topic to turn rows into map, so the map always contains
   * the latest value for each key. This is done for both expected output and actual output, before
   * comparison is done
   */
  MAP,
  /**
   * Compare expected output to actual output, as list comparison.
   */
  LIST
}
