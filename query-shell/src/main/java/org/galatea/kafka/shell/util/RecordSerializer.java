package org.galatea.kafka.shell.util;

import com.apple.foundationdb.tuple.Tuple;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.galatea.kafka.starter.util.Pair;

@Slf4j
public class RecordSerializer {

  public static byte[] key(ConsumerRecord<GenericRecord, GenericRecord> record,
      boolean compact) {
    if (compact) {
      return Tuple.from(record.key().toString()).pack();
    } else {
      return Tuple.from(record.partition(), record.offset()).pack();
    }
  }

  public static byte[] value(ConsumerRecord<GenericRecord, GenericRecord> record) {
    return Tuple.from(record.partition(), record.offset(), record.timestamp(),
        Pair.of(record.key(), record.value()).toString()).pack();
  }

  public static byte[] topicName(String topicName, int partition) {
    return Tuple.from(topicName, partition).pack();
  }

  public static byte[] offset(Long offset) {
    return Tuple.from(offset).pack();
  }
}
