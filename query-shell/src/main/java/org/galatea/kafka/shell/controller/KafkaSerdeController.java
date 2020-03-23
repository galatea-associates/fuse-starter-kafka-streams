package org.galatea.kafka.shell.controller;

import com.apple.foundationdb.tuple.Tuple;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.galatea.kafka.shell.config.MessagingConfig;
import org.galatea.kafka.shell.domain.DbRecord;
import org.galatea.kafka.shell.domain.DbRecordKey;
import org.galatea.kafka.shell.domain.SerdeType;
import org.galatea.kafka.starter.messaging.AvroSerdeUtil;
import org.galatea.kafka.starter.util.Pair;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaSerdeController {

  private final Map<String, Pair<SerdeType, SerdeType>> topicTypes = new HashMap<>();
  private final Serde<GenericRecord> avroKeySerde;
  private final Serde<GenericRecord> avroValueSerde;
  private final static Serde<Long> LONG_SERDE = Serdes.Long();
  private final static Serde<Integer> INTEGER_SERDE = Serdes.Integer();
  private final static Serde<Short> SHORT_SERDE = Serdes.Short();
  private final static Serde<Float> FLOAT_SERDE = Serdes.Float();
  private final static Serde<Double> DOUBLE_SERDE = Serdes.Double();
  private final static Serde<String> STRING_SERDE = Serdes.String();
  private final static Serde<ByteBuffer> BYTE_BUFFER_SERDE = Serdes.ByteBuffer();
  private final static Serde<Bytes> BYTES_SERDE = Serdes.Bytes();
  private final static Serde<byte[]> BYTE_ARRAY_SERDE = Serdes.ByteArray();


  public boolean isValidType(String type) {
    return Arrays.stream(SerdeType.values()).map(SerdeType::name)
        .anyMatch(name -> name.equalsIgnoreCase(type));
  }

  public List<String> validTypes() {
    return Arrays.stream(SerdeType.values()).map(SerdeType::name).collect(Collectors.toList());
  }

  public KafkaSerdeController(MessagingConfig messagingConfig) {
    avroKeySerde = AvroSerdeUtil.genericKeySerde(messagingConfig.getSchemaRegistryUrl());
    avroValueSerde = AvroSerdeUtil.genericValueSerde(messagingConfig.getSchemaRegistryUrl());
  }

  public void registerTopicTypes(String topic, SerdeType keyType, SerdeType valueType) {
    topicTypes.put(topic, Pair.of(keyType, valueType));
  }

  public ConsumerRecord<DbRecordKey, DbRecord> deserialize(ConsumerRecord<byte[], byte[]> record)
      throws Exception {
    if (!topicTypes.containsKey(record.topic())) {
      throw new IllegalStateException(
          String.format("Topic %s does not have registered serdes", record.topic()));
    }
    Pair<SerdeType, SerdeType> serdes = topicTypes.get(record.topic());
    SerdeType keyType = serdes.getKey();
    RecordMetadata metadata = new RecordMetadata(record.offset(), record.partition(),
        record.timestamp(), record.topic());
    DbRecordKey key = null;
    DbRecord value = null;
    Exception firstException = null;
    try {
      key = fromBytes(keyType, record.key(), metadata, true, DbRecordKey::new,
          (inKey, res) -> inKey.getByteKey().setObject(res.toString()));

      key.getPartition().set((long) metadata.getPartition());
      key.getOffset().set(metadata.getOffset());
    } catch (Exception e) {
      log.warn("Unable to deserialize key {}", record.key(), e);
      firstException = e;
    }

    try {
      value = fromBytes(serdes.getValue(), record.value(), metadata, false, DbRecord::new,
          (inValue, res) -> inValue.getStringValue().setObject(res.toString()));
      value.getPartition().set(metadata.getPartition());
      value.getOffset().set(metadata.getOffset());
      value.getRecordTimestamp().set(metadata.getTimestamp());
    } catch (Exception e) {
      log.warn("Unable to deserialize value {}", record.value(), e);
      firstException = firstException == null ? e : firstException;
    }
    if (firstException != null) {
      throw firstException;
    }

    return new ConsumerRecord<>(record.topic(), record.partition(), record.offset(),
        record.timestamp(), record.timestampType(), record.checksum(), record.serializedKeySize(),
        record.serializedValueSize(), key, value);
  }

  private <T> T useDeserializer(Serde<T> serde, String topic, byte[] bytes) {
    return serde.deserializer().deserialize(topic, bytes);
  }

  private <T> T fromBytes(SerdeType type, byte[] bytes, RecordMetadata metadata, boolean isKey,
      Callable<T> creator, BiConsumer<T, Object> valueSetter) throws Exception {

    T bean = creator.call();
    switch (type) {
      case AVRO:
        if (isKey) {
          valueSetter.accept(bean, useDeserializer(avroKeySerde, metadata.getTopic(), bytes));
        } else {
          valueSetter.accept(bean, useDeserializer(avroValueSerde, metadata.getTopic(), bytes));
        }
        break;
      case LONG:
        valueSetter.accept(bean, useDeserializer(LONG_SERDE, metadata.getTopic(), bytes));
        break;
      case BYTES:
        valueSetter.accept(bean, useDeserializer(BYTES_SERDE, metadata.getTopic(), bytes));
        break;
      case FLOAT:
        valueSetter.accept(bean, useDeserializer(FLOAT_SERDE, metadata.getTopic(), bytes));
        break;
      case SHORT:
        valueSetter.accept(bean, useDeserializer(SHORT_SERDE, metadata.getTopic(), bytes));
        break;
      case DOUBLE:
        valueSetter.accept(bean, useDeserializer(DOUBLE_SERDE, metadata.getTopic(), bytes));
        break;
      case STRING:
        valueSetter.accept(bean, useDeserializer(STRING_SERDE, metadata.getTopic(), bytes));
        break;
      case INTEGER:
        valueSetter.accept(bean, useDeserializer(INTEGER_SERDE, metadata.getTopic(), bytes));
        break;
      case BYTE_ARRAY:
        valueSetter.accept(bean, useDeserializer(BYTE_ARRAY_SERDE, metadata.getTopic(), bytes));
        break;
      case BYTEBUFFER:
        valueSetter.accept(bean, useDeserializer(BYTE_BUFFER_SERDE, metadata.getTopic(), bytes));
        break;
      case TUPLE:
        valueSetter.accept(bean, Tuple.fromBytes(bytes).toString());
        break;
    }

    return bean;
  }

  @Value
  private static class RecordMetadata {

    private long offset;
    private int partition;
    private long timestamp;
    private String topic;
  }
}
