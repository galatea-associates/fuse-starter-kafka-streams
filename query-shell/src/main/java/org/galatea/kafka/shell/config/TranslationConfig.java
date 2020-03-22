package org.galatea.kafka.shell.config;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KeyValue;
import org.galatea.kafka.shell.domain.DbRecord;
import org.galatea.kafka.shell.domain.DbRecordKey;
import org.galatea.kafka.starter.util.Translator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TranslationConfig {

  @Bean
  Translator<ConsumerRecord<GenericRecord, GenericRecord>, KeyValue<DbRecordKey, DbRecord>> localRecordTranslator() {
    return record -> {
      DbRecordKey key = new DbRecordKey();
      key.getOffset().setInner(record.offset());
      key.setByteKey(record.key().toString());
      key.getPartition().setInner(record.partition());

      DbRecord value = new DbRecord();
      value.getRecordTimestamp().setInner(record.timestamp());
      value.getPartition().setInner(record.partition());
      value.getOffset().setInner(record.offset());
      value.getStringValue().setInner("{Key: " + record.key() + ", Value: " + record.value() + "}");

      return KeyValue.pair(key, value);
    };
  }
}
