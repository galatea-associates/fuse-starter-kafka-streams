package org.galatea.kafka.shell.util;

import org.galatea.kafka.shell.domain.RecordMetadata;

public interface ToStringDeserializer {

  String apply(RecordMetadata recordMetadata, byte[] bytes) throws Exception;
}
