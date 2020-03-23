package org.galatea.kafka.shell.config;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.galatea.kafka.shell.domain.DbRecord;
import org.galatea.kafka.shell.domain.DbRecordKey;
import org.galatea.kafka.starter.util.Pair;
import org.galatea.kafka.starter.util.Translator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TranslationConfig {

  @Bean
  Translator<ConsumerRecord<GenericRecord, GenericRecord>, Pair<DbRecordKey, DbRecord>> localRecordTranslator() {
    return record -> {
      DbRecordKey key = new DbRecordKey();
      key.getOffset().setObject(record.offset());
      key.setByteKey(record.key().toString());
      key.getPartition().setObject(record.partition());

      DbRecord value = new DbRecord();
      value.getRecordTimestamp().setObject(record.timestamp());
      value.getPartition().setObject(record.partition());
      value.getOffset().setObject(record.offset());
      value.getStringValue()
          .setObject("{Key: " + record.key() + ", Value: " + record.value() + "}");

      return Pair.of(key, value);
    };
  }
}
