package org.galatea.kafka.starter.messaging.streams;

@FunctionalInterface
public interface StateInitializer<T> {
  T init();
}
