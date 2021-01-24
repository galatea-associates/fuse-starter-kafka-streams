package org.galatea.kafka.starter.testing.avro;

public interface RecordPostProcessor<T> {

  T process(T record);
}
