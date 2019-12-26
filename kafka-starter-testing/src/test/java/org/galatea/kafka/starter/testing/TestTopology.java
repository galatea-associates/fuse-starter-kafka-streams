package org.galatea.kafka.starter.testing;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.galatea.kafka.starter.messaging.Topic;

@Slf4j
public class TestTopology {

  public static final Topic<String, String> inputTopic = new Topic<>("input", Serdes.String(),
      Serdes.String());
  public static final Topic<String, String> outputTopic = new Topic<>("output", Serdes.String(),
      Serdes.String());
  public static final String STORE_NAME = "store";

  public static Topology topology() {
    StreamsBuilder builder = new StreamsBuilder();

    builder.addStateStore(Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore(STORE_NAME), Serdes.String(),
            Serdes.String()));

    builder.stream(inputTopic.getName(), inputTopic.consumedWith())
        .peek(TestTopology::logConsume)
        .transform(
            () -> new Transformer<String, String, KeyValue<String, String>>() {
              private KeyValueStore<String, String> store;

              @Override
              public void init(ProcessorContext context) {
                store = (KeyValueStore<String, String>) context.getStateStore(STORE_NAME);
              }

              @Override
              public KeyValue<String, String> transform(String key, String value) {
                String existingEntry = store.get(key);
                String newValue;
                if (existingEntry == null || Character.isLowerCase(existingEntry.charAt(0))) {
                  newValue = value.toUpperCase();
                } else {
                  newValue = existingEntry.toLowerCase();
                }

                store.put(key, newValue);
                return new KeyValue<>(key, newValue);
              }

              @Override
              public void close() {

              }
            }, STORE_NAME)
        .peek(TestTopology::logProduce)
        .to(outputTopic.getName(), outputTopic.producedWith());

    return builder.build();
  }

  private static void logConsume(Object key, Object value) {
    log.info("Consumed [{}|{}]: {} | {}", readableClassOf(key), readableClassOf(value), key, value);
  }

  private static void logProduce(Object key, Object value) {
    log.info("Produced [{}|{}]: {} | {}", readableClassOf(key), readableClassOf(value), key, value);
  }

  private static String readableClassOf(Object obj) {
    return obj == null ? "N/A" : obj.getClass().getSimpleName();
  }

}
