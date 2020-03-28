package org.galatea.kafka.shell.controller;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.galatea.kafka.shell.domain.ConsumerProperties;
import org.galatea.kafka.shell.domain.PartitionConsumptionStatus;
import org.galatea.kafka.shell.domain.ShellEntityType;
import org.galatea.kafka.shell.domain.StoreStatus;
import org.galatea.kafka.shell.domain.TopicPartitionOffsets;
import org.galatea.kafka.shell.stores.ConsumerRecordTable;
import org.galatea.kafka.starter.util.Pair;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class StatusController {

  private final RecordStoreController recordStoreController;
  private final ConsumerThreadController consumerThreadController;
  private final AdminClient adminClient;
  private final SchemaRegistryController schemaRegistryController;
  private final NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.US);

  private Map<String, StoreStatus> storeStatus() {

    return recordStoreController.getTables().values().stream()
        .map(store -> Pair.of(store.getTable().getName(), store.getTable().status()))
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

  public String printableDetails(ShellEntityType type, String name, String[] parameters)
      throws ExecutionException, InterruptedException, IOException, RestClientException {
    switch (type) {
      case TOPIC:
        return describeTopic(name);
      case STORE:
        return describeStore(name);
      case SCHEMA:
        return describeSchema(name, parameters);
      default:
        return String.format("Unknown entity type %s", type);
    }
  }

  private String describeSchema(String name, String[] parameters)
      throws IOException, RestClientException {

    StringBuilder sb = new StringBuilder();
    Optional<Integer> version;
    if (parameters.length == 0) {
      sb.append("Retrieving latest version of schema since version was not specified\n");
      version = schemaRegistryController.getLatesSchemaMetadata(name).map(SchemaMetadata::getVersion);
    } else {
      if (parameters.length > 1) {
        sb.append(String.format("Using first parameter '%s' as version number", parameters[0]));
      }
      version = Optional.of(Integer.parseInt(parameters[0]));
    }

    Optional<Schema> schema = Optional.empty();
    if (version.isPresent()) {
      schema = schemaRegistryController.describeSchema(name, version.get());
    }
    if (schema.isPresent()) {
      sb.append(schema.get().toString(true));
    } else {
      sb.append("Could not find schema");
    }
    return sb.toString();
  }

  private String describeStore(String name) {
    ConsumerRecordTable table = recordStoreController.getTable(name);
    // since name may be an alias
    String tableName = table.getName();
    List<String> topics = consumerThreadController.consumerProperties().getStoreSubscription()
        .entrySet().stream()
        .filter(e -> e.getValue().stream().anyMatch(t -> t.getName().equals(tableName)))
        .map(Entry::getKey)
        .collect(Collectors.toList());

    List<List<String>> printTable = new ArrayList<>();
    printTable.add(Arrays.asList("Property", "Value"));
    printTable.add(Arrays.asList("Name", tableName));
    printTable.add(Arrays.asList("Topics", topics.toString()));
    printTable.add(Arrays.asList("Alias", recordStoreController.aliasFor(tableName).orElse("")));
    printTable
        .add(Arrays.asList("Received Records", numberFormat.format(table.getRecordsReceived())));
    printTable.add(Arrays.asList("Unique Keys", numberFormat.format(table.getRecordsInStore())));
    printTable.add(Arrays.asList("Filter", Arrays.toString(table.getRecordFilter().getRegex())));

    return printableTable(printTable);
  }

  private String describeTopic(String name) throws ExecutionException, InterruptedException {
    DescribeTopicsResult result = adminClient
        .describeTopics(Collections.singleton(name));
    TopicDescription description = result.all().get().get(name);
    StringBuilder sb = new StringBuilder();
    sb.append("Topic: ").append(description.name())
        .append(";\tPartitions: ").append(description.partitions().size())
        .append(";\tInternal: ").append(description.isInternal()).append("\n")
        .append("Partitions:\n");

    List<TopicPartitionInfo> partitions = description.partitions().stream()
        .sorted(Comparator.comparing(TopicPartitionInfo::partition)).collect(Collectors.toList());
    List<List<String>> partitionTable = new ArrayList<>();
    partitionTable.add(Arrays.asList("Partition", "Leader", "ISR", "Replicas"));
    for (TopicPartitionInfo partition : partitions) {
      partitionTable.add(Arrays
          .asList(String.valueOf(partition.partition()),
              String.valueOf(partition.leader().id()),
              partition.isr().stream().map(Node::id).collect(Collectors.toList()).toString(),
              partition.replicas().stream().map(Node::id).collect(Collectors.toList()).toString()));
    }
    sb.append(printableTable(partitionTable));

    return sb.toString();
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
    table.add(Arrays.asList("Name", "# Records", "Alias"));
    storeStatus().forEach(
        (key, value) -> table
            .add(Arrays.asList(key, numberFormat.format(value.getMessagesInStore()),
                recordStoreController.aliasFor(key).orElse(""))));
    sb.append(printableTable(table));

    sb.append("Consumer Topics :\n");
    List<List<String>> topicTable = new ArrayList<>();
    topicTable.add(Arrays.asList("Topic", "Lag", "# Consumed"));
    consumerLagByTopic().forEach((topic, stat) -> topicTable
        .add(Arrays.asList(topic, numberFormat.format(stat.getLag()),
            numberFormat.format(stat.getConsumedMessages()))));
    sb.append(printableTable(topicTable));
    sb.append(String
        .format("%.2f msg/s\n",
            consumerThreadController.consumerProperties().getHistoricalStatistic()
                .messagesPerSecond()));

    Map<String, Exception> topicExceptions = consumerThreadController.consumerProperties()
        .getTopicExceptions();
    if (!topicExceptions.isEmpty()) {
      sb.append("Exceptions:\n");
      List<List<String>> errorTable = new ArrayList<>();
      errorTable.add(Arrays.asList("Topic", "Message"));

      topicExceptions
          .forEach((topic, e) -> errorTable.add(Arrays.asList(topic, stringStackTrace(e))));
      sb.append(printableTable(errorTable));
      topicExceptions.clear();
    }

    return sb.toString();
  }

  private String stringStackTrace(Exception e) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    return sw.toString();
  }

  private String printableTable(List<List<String>> table) {
    if (table.size() == 0) {
      return "";
    }

    // handle cells with multiple lines
    for (int rowNum = 0; rowNum < table.size(); rowNum++) {
      List<String> row = table.get(rowNum);
      int maxLines = 1;
      for (String cell : row) {
        String[] lines = cell.split("\n");
        maxLines = Math.max(lines.length, maxLines);
      }
      for (int i = 1; i < maxLines; i++) {
        table.add(rowNum + 1, rowWithBlanks(row.size()));
      }
      if (maxLines > 1) {
        for (int cellNum = 0; cellNum < row.size(); cellNum++) {
          String cell = row.get(cellNum);
          String[] lines = cell.split("\n");
          for (int i = 0; i < lines.length; i++) {
            table.get(rowNum + i).set(cellNum, lines[i]);
          }
        }
      }
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

  private List<String> rowWithBlanks(int size) {
    ArrayList<String> output = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      output.add("");
    }
    return output;
  }
}
