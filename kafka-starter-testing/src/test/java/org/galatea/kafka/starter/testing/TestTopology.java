package org.galatea.kafka.starter.testing;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.test.TestMsgKey;
import org.galatea.kafka.starter.messaging.test.TestMsgValue;

@Slf4j
public class TestTopology {

  private static final SchemaRegistryClient mockSchemaRegistry = new MockSchemaRegistryClient();
  private static final Serde<TestMsgKey> keySerde = new SpecificAvroSerde<>(mockSchemaRegistry);
  private static final Serde<TestMsgValue> valueSerde = new SpecificAvroSerde<>(mockSchemaRegistry);

  static {
    keySerde.configure(Collections
        .singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            "http://localhost:65535"), true);
    valueSerde.configure(Collections
        .singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            "http://localhost:65535"), false);

  }

  public static final Topic<String, String> inputTopic = new Topic<>("input1", Serdes.String(),
      Serdes.String());
  public static final Topic<TestMsgKey, TestMsgValue> inputTopic2 = new Topic<>("input2", keySerde,
      valueSerde);
  public static final Topic<String, String> outputTopic = new Topic<>("output1", Serdes.String(),
      Serdes.String());
  public static final Topic<TestMsgKey, TestMsgValue> outputTopic2 = new Topic<>("output2",
      keySerde, valueSerde);
  public static final String STORE_NAME1 = "store1";
  public static final String STORE_NAME2 = "store2";

  public static Topology topology() {
    StreamsBuilder builder = new StreamsBuilder();

    builder.addStateStore(Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore(STORE_NAME1), Serdes.String(),
            Serdes.String()));

    builder.addStateStore(Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore(STORE_NAME2),
            inputTopic2.getKeySerde(), inputTopic2.getValueSerde()));

    builder.stream(inputTopic2.getName(), inputTopic2.consumedWith())
        .peek(TestTopology::logConsume)
        .transform(
            () -> new Transformer<TestMsgKey, TestMsgValue, KeyValue<TestMsgKey, TestMsgValue>>() {
              private KeyValueStore<TestMsgKey, TestMsgValue> store;

              @Override
              public void init(ProcessorContext context) {
                store = (KeyValueStore<TestMsgKey, TestMsgValue>) context
                    .getStateStore(STORE_NAME2);
              }

              @Override
              public KeyValue<TestMsgKey, TestMsgValue> transform(TestMsgKey key,
                  TestMsgValue value) {
                TestMsgValue existingValue = store.get(key);
                if (existingValue == null) {
                  existingValue = value;
                }

                String existingString = existingValue.getNonNullableStringField();
                if (Character.isLowerCase(existingString.charAt(0))) {
                  existingString = existingString.toUpperCase();
                } else {
                  existingString = existingString.toLowerCase();
                }
                existingValue.setNonNullableStringField(existingString);
                store.put(key, existingValue);
                return KeyValue.pair(key, existingValue);
              }

              @Override
              public void close() {

              }
            }, STORE_NAME2)
        .peek(TestTopology::logProduce)
        .to(outputTopic2.getName(), outputTopic2.producedWith());

    builder.stream(inputTopic.getName(), inputTopic.consumedWith())
        .peek(TestTopology::logConsume)
        .transform(
            () -> new Transformer<String, String, KeyValue<String, String>>() {
              private KeyValueStore<String, String> store;

              @Override
              public void init(ProcessorContext context) {
                store = (KeyValueStore<String, String>) context.getStateStore(STORE_NAME1);
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
            }, STORE_NAME1)
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
