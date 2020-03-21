package org.galatea.kafka.shell.domain;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.Data;
import org.apache.kafka.common.TopicPartition;
import org.galatea.kafka.shell.stores.OffsetTrackingRecordStore;

@Data
public class ConsumerProperties {

  private boolean stopTriggered = false;
  private boolean assignmentUpdated = false;
  private Set<TopicPartition> assignment = new HashSet<>();
  private Set<TopicPartition> seekBeginningAssignment = new HashSet<>();
  private Map<String, Set<OffsetTrackingRecordStore>> storeSubscription = new HashMap<>();
  private Map<TopicPartition, Long> latestOffset = new HashMap<>();
  private Map<TopicPartition, Long> consumedMessages = new HashMap<>();

  private BlockingQueue<ConsumerRequest<?>> pendingRequests = new LinkedBlockingQueue<>();
}
