package org.galatea.kafka.starter.messaging.kafka.consume;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ConsumerThread extends Thread implements Closeable {

  @NotNull
  private final ConsumerFactory factory;
  @NotNull
  private final Properties consumerProps;
  @NotNull
  private final AssignmentInstruction assignmentInstruction;
  @Nullable
  private final BatchFinalizer batchFinalizer;
  @NotNull
  private final Duration pollDuration;
  @NotNull
  private final java.util.function.Consumer<ConsumerRecord<byte[], byte[]>> handleRecords;
  private boolean stopRequested = false;

  @Builder
  public ConsumerThread(@NotNull ConsumerFactory factory, @NotNull Properties consumerProps,
      @NotNull ConsumerThread.AssignmentInstruction assignmentInstruction,
      @Nullable ConsumerThread.BatchFinalizer batchFinalizer,
      @NotNull Duration pollDuration,
      java.util.function.Consumer<ConsumerRecord<byte[], byte[]>> handleRecords) {
    this.factory = factory;
    this.consumerProps = consumerProps;
    this.assignmentInstruction = assignmentInstruction;
    this.batchFinalizer = batchFinalizer;
    this.pollDuration = pollDuration;
    this.handleRecords = handleRecords;
  }


  @Override
  public void run() {
    Optional<BatchFinalizer> finalizer = Optional.ofNullable(this.batchFinalizer);
    Deserializer<byte[]> serde = Serdes.ByteArray().deserializer();
    try (Consumer<byte[], byte[]> consumer = factory.create(consumerProps, serde, serde)) {
      assignmentInstruction.apply(consumer);

      while (!Thread.interrupted() && !stopRequested) {

        consumer.poll(pollDuration).forEach(handleRecords);

        finalizer.ifPresent(c -> c.apply(consumer));
      }
    }
  }

  @Override
  public void close() {
    stopRequested = true;
  }

  public interface AssignmentInstruction {

    /**
     * Define actions that will be taken after consumer creation but before starting the {@link
     * Consumer#poll(Duration)} loop. Typically this will be either subscription to topics or
     * topic/partition assignment. May also include seek instructions.
     */
    void apply(Consumer<?, ?> consumer);

    static AssignmentInstruction subscribe(Collection<String> topics) {
      return consumer -> consumer.subscribe(topics);
    }

    static AssignmentInstruction assign(Collection<TopicPartition> partitions) {
      return consumer -> consumer.assign(partitions);
    }

    static AssignmentInstruction assignAllPartitions(Collection<String> topics) {

      Set<String> topicSet = new HashSet<>(topics);
      return consumer -> {
        List<TopicPartition> partitions = consumer.listTopics().entrySet().stream()
            .filter(e -> topicSet.contains(e.getKey()))
            .flatMap(e -> e.getValue().stream()
                .map(p -> new TopicPartition(p.topic(), p.partition())))
            .collect(Collectors.toList());
        consumer.assign(partitions);
      };
    }

  }

  public interface BatchFinalizer {

    /**
     * Define actions that will be taken after all records in a batch are complete. This will
     * typically consist of acknowledging records if auto-ack is not enabled.
     */
    void apply(Consumer<?, ?> consumer);
  }
}
