package org.galatea.kafka.starter.messaging.serialize.domain;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.galatea.kafka.starter.messaging.serialize.TupleKey;
import org.galatea.kafka.starter.messaging.serialize.annotation.TupleKeyField;

@ToString
// NoArgsConstructor required by serde, use of the constructor discouraged
@NoArgsConstructor(onConstructor = @__({@Deprecated}))
@AllArgsConstructor(access = AccessLevel.PACKAGE)   // required for builder
@Builder
@Getter
@EqualsAndHashCode
public class TripleValueKey implements TupleKey {

  @TupleKeyField(1)
  private String field1;
  @TupleKeyField(2)
  private String field2;
  @TupleKeyField(3)
  private String field3;
}
