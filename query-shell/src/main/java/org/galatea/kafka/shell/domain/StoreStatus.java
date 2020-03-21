package org.galatea.kafka.shell.domain;


import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
@RequiredArgsConstructor
public class StoreStatus {

  private final long messagesConsumed;
  private final long messagesInStore;
  private final Map<Integer, Long> lastOffset;
}
