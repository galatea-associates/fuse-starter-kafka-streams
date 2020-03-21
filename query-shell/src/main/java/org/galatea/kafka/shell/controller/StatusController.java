package org.galatea.kafka.shell.controller;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.galatea.kafka.shell.consumer.ConsumerThreadController;
import org.galatea.kafka.shell.domain.ConsumerProperties;
import org.galatea.kafka.shell.domain.PartitionConsumptionStatus;
import org.galatea.kafka.shell.domain.StoreStatus;
import org.galatea.kafka.shell.domain.TopicPartitionOffsets;
import org.galatea.kafka.starter.util.Pair;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class StatusController {

  private final RecordStoreController recordStoreController;
  private final ConsumerThreadController consumerThreadController;
  private final NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.US);

  private Map<String, StoreStatus> storeStatus() {

    return recordStoreController.getStores().values().stream()
        .map(store -> Pair.of(store.getStoreName(), store.status()))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  private Map<TopicPartition, PartitionConsumptionStatus> consumerStatus() {

    ConsumerProperties properties = consumerThreadController.consumerProperties();
    Map<TopicPartition, PartitionConsumptionStatus> consumerStatus = new HashMap<>();
    properties.getAssignment().forEach(topicPartition -> {
      PartitionConsumptionStatus status = new PartitionConsumptionStatus();
      consumerStatus.put(topicPartition, status);

      if (properties.getConsumedMessages().containsKey(topicPartition)) {
        status.setConsumedMessages(properties.getConsumedMessages().get(topicPartition));
      }
      if (properties.getLatestOffset().containsKey(topicPartition)) {
        status.setLatestOffsets(properties.getLatestOffset().get(topicPartition));
      }
    });

    return consumerStatus;
  }

  private Map<TopicPartition, TopicPartitionOffsets> topicStatus() throws InterruptedException {
    return consumerThreadController.consumerStatus();
  }

  @Data
  private static class ConsumerStat {

    long lag = 0;
    long consumedMessages = 0;
  }

  private Map<String, ConsumerStat> consumerLagByTopic() throws InterruptedException {
    Map<TopicPartition, TopicPartitionOffsets> topicStatus = topicStatus();
    Map<TopicPartition, PartitionConsumptionStatus> consumerStatus = consumerStatus();

    Map<String, ConsumerStat> outputMap = new HashMap<>();
    topicStatus.forEach((topicPartition, partitionOffsets) -> {

      PartitionConsumptionStatus partitionConsumptionStatus = consumerStatus.get(topicPartition);
      long latestOffsetSeen = 1;
      long consumedMessages = 0;
      if (partitionConsumptionStatus != null) {
        latestOffsetSeen = partitionConsumptionStatus.getLatestOffsets();
        consumedMessages = partitionConsumptionStatus.getConsumedMessages();
      }
      latestOffsetSeen = Math.max(partitionOffsets.getBeginningOffset(), latestOffsetSeen);

      ConsumerStat stat = outputMap
          .computeIfAbsent(topicPartition.topic(), s -> new ConsumerStat());
      if (partitionOffsets.getEndOffset() != partitionOffsets.getBeginningOffset()) {
        stat.setLag(stat.getLag() + partitionOffsets.getEndOffset() - latestOffsetSeen - 1);
      }
      stat.setConsumedMessages(stat.getConsumedMessages() + consumedMessages);
    });

    return outputMap;
  }

  public String printableStatus() throws InterruptedException {
    StringBuilder sb = new StringBuilder();
    sb.append("Stores:\n");
    List<List<String>> table = new ArrayList<>();
    table.add(Arrays.asList("Name", "# Records"));
    storeStatus().forEach(
        (key, value) -> table
            .add(Arrays.asList(key, numberFormat.format(value.getMessagesInStore()))));
    sb.append(printableTable(table));

    sb.append("Consumer Topics:\n");
    List<List<String>> topicTable = new ArrayList<>();
    topicTable.add(Arrays.asList("Topic", "Lag", "# Consumed"));
    consumerLagByTopic().forEach((topic, stat) -> topicTable
        .add(Arrays.asList(topic, numberFormat.format(stat.getLag()),
            numberFormat.format(stat.getConsumedMessages()))));
    sb.append(printableTable(topicTable));

    return sb.toString();
  }

  private String printableTable(List<List<String>> table) {
    if (table.size() == 0) {
      return "";
    }
    Integer[] maxLengthForColumn = new Integer[table.get(0).size()];
    Arrays.fill(maxLengthForColumn, 0);

    for (List<String> row : table) {
      for (int colNum = 0; colNum < row.size(); colNum++) {
        String cell = row.get(colNum);
        if (cell.length() > maxLengthForColumn[colNum]) {
          maxLengthForColumn[colNum] = cell.length();
        }
      }
    }
    StringBuilder sb = new StringBuilder();
    int columns =
        Arrays.stream(maxLengthForColumn).mapToInt(value -> value).sum() + table.get(0).size() * 3
            - 1;
    sb.append("+");
    for (int i = 0; i < columns; i++) {
      sb.append("-");
    }
    sb.append("+\n");

    for (List<String> row : table) {
      StringBuilder lineBuilder = new StringBuilder("|");
      for (int i = 0; i < row.size(); i++) {
        lineBuilder.append(" %-").append(maxLengthForColumn[i]).append("s |");
      }
      sb.append(String.format(lineBuilder.toString(), row.toArray())).append("\n");
    }
    sb.append("+");
    for (int i = 0; i < columns; i++) {
      sb.append("-");
    }
    sb.append("+\n");
    return sb.toString();
  }
}
