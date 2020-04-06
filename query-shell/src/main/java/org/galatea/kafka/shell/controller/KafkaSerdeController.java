package org.galatea.kafka.shell.controller;

import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.galatea.kafka.shell.domain.DbRecord;
import org.galatea.kafka.shell.domain.DbRecordKey;
import org.galatea.kafka.shell.domain.RecordMetadata;
import org.galatea.kafka.shell.domain.SerdeType;
import org.galatea.kafka.shell.domain.SerdeType.DataType;
import org.galatea.kafka.shell.util.ToStringDeserializer;
import org.galatea.kafka.starter.util.Pair;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaSerdeController {

  private final Map<String, Pair<DataType, DataType>> topicTypes = new HashMap<>();
  private final Map<SerdeType, ToStringDeserializer> registeredSerdes;

  public void registerTopicTypes(String topic, DataType keyType, DataType valueType) {
    topicTypes.put(topic, Pair.of(keyType, valueType));
  }

  public ConsumerRecord<DbRecordKey, DbRecord> deserialize(ConsumerRecord<byte[], byte[]> record)
      throws Exception {
    if (!topicTypes.containsKey(record.topic())) {
      throw new IllegalStateException(
          String.format("Topic %s does not have registered serdes", record.topic()));
    }

    Pair<DataType, DataType> topicTypes = this.topicTypes.get(record.topic());

    ToStringDeserializer keyConversion = registeredSerdes
        .get(new SerdeType(true, topicTypes.getKey()));
    ToStringDeserializer valueConversion = registeredSerdes
        .get(new SerdeType(false, topicTypes.getValue()));

    RecordMetadata metadata = new RecordMetadata(record.offset(), record.partition(),
        record.timestamp(), record.topic());
    DbRecordKey key = new DbRecordKey();
    key.getStringKey().set(keyConversion.apply(metadata, record.key()));
    key.getOffset().set(record.offset());
    key.getPartition().set(record.partition());

    DbRecord value = new DbRecord();
    value.getStringValue().set(valueConversion.apply(metadata, record.value()));
    value.getRecordTimestamp().set(record.timestamp());
    value.getOffset().set(record.offset());
    value.getPartition().set(record.partition());

    return new ConsumerRecord<>(record.topic(), record.partition(), record.offset(),
        record.timestamp(), record.timestampType(), record.checksum(), record.serializedKeySize(),
        record.serializedValueSize(), key, value);
  }

}
