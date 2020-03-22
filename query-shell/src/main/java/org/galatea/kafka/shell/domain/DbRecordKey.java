package org.galatea.kafka.shell.domain;

import com.apple.foundationdb.tuple.Tuple;
import lombok.Value;

@Value
public class DbRecordKey {

  private MutableField<Long> partition = new MutableField<>(-1L);
  private MutableField<Long> offset = new MutableField<>(-1L);
  private MutableField<byte[]> byteKey = new MutableField<>(null);

  public void setByteKey(String key) {
    byteKey.setInner(Tuple.from(key).pack());
  }
}
