package org.galatea.kafka.shell.config;

import com.apple.foundationdb.tuple.Tuple;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.galatea.kafka.shell.domain.RecordMetadata;
import org.galatea.kafka.shell.domain.SerdeType;
import org.galatea.kafka.shell.domain.SerdeType.DataType;
import org.galatea.kafka.shell.util.ToStringDeserializer;
import org.galatea.kafka.starter.messaging.AvroSerdeUtil;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class KafkaSerdeConfig {

  private final static Serde<Long> LONG_SERDE = Serdes.Long();
  private final static Serde<Integer> INTEGER_SERDE = Serdes.Integer();
  private final static Serde<Short> SHORT_SERDE = Serdes.Short();
  private final static Serde<Float> FLOAT_SERDE = Serdes.Float();
  private final static Serde<Double> DOUBLE_SERDE = Serdes.Double();
  private final static Serde<String> STRING_SERDE = Serdes.String();
  private final static Serde<ByteBuffer> BYTE_BUFFER_SERDE = Serdes.ByteBuffer();
  private final static Serde<Bytes> BYTES_SERDE = Serdes.Bytes();
  private final static Serde<byte[]> BYTE_ARRAY_SERDE = Serdes.ByteArray();

  @Bean
  public Serde<GenericRecord> avroKeySerde(MessagingConfig messagingConfig) {
    return AvroSerdeUtil.genericKeySerde(messagingConfig.getSchemaRegistryUrl());
  }

  @Bean
  public Serde<GenericRecord> avroValueSerde(MessagingConfig messagingConfig) {
    return AvroSerdeUtil.genericValueSerde(messagingConfig.getSchemaRegistryUrl());
  }

  @Bean
  public Map<SerdeType, ToStringDeserializer> registeredSerdes(
      Serde<GenericRecord> avroKeySerde, Serde<GenericRecord> avroValueSerde) {

    Map<SerdeType, ToStringDeserializer> serdeMap = new HashMap<>();
    serdeMap.put(new SerdeType(true, DataType.AVRO), (metadata, bytes) -> {
      GenericRecord genericRecord = deserializeAvro(avroKeySerde, metadata, bytes);
      return genericRecord == null ? null : genericRecord.toString();
    });
    serdeMap.put(new SerdeType(false, DataType.AVRO), (metadata, bytes) -> {
      GenericRecord genericRecord = deserializeAvro(avroValueSerde, metadata, bytes);
      return genericRecord == null ? null : genericRecord.toString();
    });

    addPrimitiveSerde(serdeMap, DataType.LONG, LONG_SERDE);
    addPrimitiveSerde(serdeMap, DataType.BYTES, BYTES_SERDE);
    addPrimitiveSerde(serdeMap, DataType.FLOAT, FLOAT_SERDE);
    addPrimitiveSerde(serdeMap, DataType.SHORT, SHORT_SERDE);
    addPrimitiveSerde(serdeMap, DataType.DOUBLE, DOUBLE_SERDE);
    addPrimitiveSerde(serdeMap, DataType.STRING, STRING_SERDE);
    addPrimitiveSerde(serdeMap, DataType.INTEGER, INTEGER_SERDE);
    addPrimitiveSerde(serdeMap, DataType.BYTE_ARRAY, BYTE_ARRAY_SERDE);
    addPrimitiveSerde(serdeMap, DataType.BYTEBUFFER, BYTE_BUFFER_SERDE);

    addKeyAndValueTypes(serdeMap, DataType.TUPLE, (m, bytes) -> Tuple.fromBytes(bytes).toString());

    return serdeMap;
  }

  private void addPrimitiveSerde(
      Map<SerdeType, ToStringDeserializer> serdeMap, DataType type,
      Serde<?> serde) {
    addKeyAndValueTypes(serdeMap, type,
        (meta, bytes) -> useDeserializer(serde, meta.getTopic(), bytes).toString());
  }

  private static void addKeyAndValueTypes(
      Map<SerdeType, ToStringDeserializer> serdeMap, DataType type,
      ToStringDeserializer conversion) {

    serdeMap.put(new SerdeType(true, type), conversion);
    serdeMap.put(new SerdeType(false, type), conversion);
  }

  private GenericRecord deserializeAvro(Serde<GenericRecord> avroSerde, RecordMetadata metadata,
      byte[] bytes) {
    GenericRecord genericRecord = useDeserializer(avroSerde, metadata.getTopic(), bytes);
    fixLogicalTypes(genericRecord);
    return genericRecord;
  }

  private GenericRecord fixLogicalTypes(GenericRecord record) {
    if (record == null) {
      return null;
    }
    Schema schema = record.getSchema();
    for (Field field : schema.getFields()) {
      Schema fieldType = field.schema();
      if (field.schema().getType().equals(Type.UNION)) {
        fieldType = firstNonNullSchema(field.schema());
      }

      if (fieldType.getType().equals(Type.RECORD)) {
        record.put(field.pos(), fixLogicalTypes((GenericRecord) record.get(field.pos())));

      } else if (field.schema().getLogicalType() != null && record.get(field.pos()) != null) {
        Object fieldValue = record.get(field.pos());
        LogicalType logicalType = field.schema().getLogicalType();

        try {
          if (logicalType.equals(LogicalTypes.timestampMillis())) {
            record.put(field.pos(), Instant.ofEpochMilli((long) fieldValue));
          } else if (logicalType.equals(LogicalTypes.timestampMicros())) {
            record.put(field.pos(), Instant.EPOCH.plus((long) fieldValue, ChronoUnit.MICROS));
          } else if (logicalType.equals(LogicalTypes.timeMillis())) {
            record.put(field.pos(), LocalTime.ofNanoOfDay((int) fieldValue * 1000000));
          } else if (logicalType.equals(LogicalTypes.timeMicros())) {
            record.put(field.pos(), LocalTime.ofNanoOfDay((long) fieldValue * 1000));
          } else if (logicalType.equals(LogicalTypes.date())) {
            record.put(field.pos(), LocalDate.ofEpochDay((int) fieldValue));
          } else {
            throw new IllegalArgumentException(
                String.format("Unconfigured logical type %s", logicalType.toString()));
          }
        } catch (Exception e ) {
          log.error("Could not make logical type readable: {}.", field.name(), e);
        }
      }
    }
    return record;
  }

  private Schema firstNonNullSchema(Schema o) {
    return o.getTypes().stream().filter(s -> !s.getType().equals(Schema.Type.NULL))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException(
            String.format("Could not find non-null type in union of object %s", o.toString())));
  }


  private <T> T useDeserializer(Serde<T> serde, String topic, byte[] bytes) {
    return serde.deserializer().deserialize(topic, bytes);
  }

}
