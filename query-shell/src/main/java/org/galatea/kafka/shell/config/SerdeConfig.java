package org.galatea.kafka.shell.config;

import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.galatea.kafka.shell.domain.DbRecord;
import org.galatea.kafka.shell.domain.DbRecordKey;
import org.galatea.kafka.shell.serde.DbRecordSerde;
import org.galatea.kafka.shell.util.FieldExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SerdeConfig {

  private static final List<FieldExtractor<DbRecordKey>> COMPACT_KEY_PROPERTIES = Arrays
      .asList(DbRecordKey::getByteKey);
  private static final List<FieldExtractor<DbRecordKey>> ALL_KEY_PROPERTIES = Arrays
      .asList(DbRecordKey::getPartition, DbRecordKey::getOffset, DbRecordKey::getByteKey);
  private static final List<FieldExtractor<DbRecord>> VALUE_PROPERTIES = Arrays
      .asList(DbRecord::getPartition, DbRecord::getOffset, DbRecord::getRecordTimestamp,
          DbRecord::getStringValue);

  @Bean
  public Serde<DbRecordKey> compactKeySerde() {
    return new DbRecordSerde<>(COMPACT_KEY_PROPERTIES, DbRecordKey::new);
  }

  @Bean
  public Serde<DbRecordKey> allRecordKeySerde() {
    return new DbRecordSerde<>(ALL_KEY_PROPERTIES, DbRecordKey::new);
  }

  @Bean
  public Serde<DbRecord> valueSerde() {
    return new DbRecordSerde<>(VALUE_PROPERTIES, DbRecord::new);
  }
}
