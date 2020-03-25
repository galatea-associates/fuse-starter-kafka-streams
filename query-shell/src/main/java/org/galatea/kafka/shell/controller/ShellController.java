package org.galatea.kafka.shell.controller;

import java.lang.reflect.UndeclaredThrowableException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.galatea.kafka.shell.config.MessagingConfig;
import org.galatea.kafka.shell.config.StandardWithVarargsResolver;
import org.galatea.kafka.shell.domain.DbRecord;
import org.galatea.kafka.shell.domain.DbRecordKey;
import org.galatea.kafka.shell.domain.SerdeType;
import org.galatea.kafka.shell.domain.ShellEntityType;
import org.galatea.kafka.shell.stores.ConsumerRecordTable;
import org.galatea.kafka.shell.util.DbRecordStringUtil;
import org.galatea.kafka.shell.util.ListEntityFunction;
import org.galatea.kafka.shell.util.RegexPredicate;
import org.galatea.kafka.starter.util.Pair;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

@Slf4j
@ShellComponent
public class ShellController {

  // TODO: describe consumer group
  // TODO: 'query detailed' command prints (and looks for pattern) with offsets and partitions
  // TODO: limit number of records returned by query
  // TODO: figure out how to deal with cluster not available - currently hangs on start?

  private final RecordStoreController recordStoreController;
  private final ConsumerThreadController consumerThreadController;
  private final StatusController statusController;
  private final AdminClient adminClient;
  private final KafkaSerdeController kafkaSerdeController;
  private final static Map<String, ListEntityFunction> LIST_ENTITIES = new HashMap<>();

  static {
    LIST_ENTITIES.put("TOPIC",
        client -> client.listTopics().names().get().stream().sorted().collect(Collectors.toList()));
    LIST_ENTITIES.put("CONSUMER-GROUP", client -> client.listConsumerGroups().all().get().stream()
        .map(ConsumerGroupListing::groupId).collect(Collectors.toList()));
  }

  public ShellController(RecordStoreController recordStoreController,
      ConsumerThreadController consumerThreadController, StatusController statusController,
      MessagingConfig messagingConfig, AdminClient adminClient,
      KafkaSerdeController kafkaSerdeController) throws Exception {
    this.recordStoreController = recordStoreController;
    this.consumerThreadController = consumerThreadController;
    this.statusController = statusController;
    this.adminClient = adminClient;
    this.kafkaSerdeController = kafkaSerdeController;

    String clusterId = adminClient.describeCluster().clusterId().get();
    System.out
        .println(String.format("Connected to brokers: %s", messagingConfig.getBootstrapServer()));
    System.out.println(
        String.format("Connected to schema registry: %s", messagingConfig.getSchemaRegistryUrl()));
    System.out.println(String.format("Cluster ID: %s", clusterId));
  }

  @ShellMethod(value = "Stop listening to a topic", key = "stop-listen")
  public String stopListen(@ShellOption String topicName) {

    Set<ConsumerRecordTable> subscribedTables = consumerThreadController
        .removeTopicAssignment(topicName);
    subscribedTables.forEach(table -> recordStoreController.deleteTable(table.getName()));

    return String.format("Stopped consuming topic %s and deleted stores %s", topicName,
        subscribedTables.stream().map(ConsumerRecordTable::getName)
            .collect(Collectors.joining(", ")));
  }

  @ShellMethod("Get details about an entity")
  public String describe(
      @ShellOption(value = "--entity-type") ShellEntityType entityType,
      @ShellOption(value = "--entity-name") String name)
      throws ExecutionException, InterruptedException {
    return statusController.printableDetails(entityType, name);
  }

  @ShellMethod("Get status of the service")
  public String status() throws InterruptedException {
    return statusController.printableStatus();
  }

  @ShellMethod("Search a store using REGEX")
  public String query(
      @ShellOption String storeName,
      @ShellOption(arity = StandardWithVarargsResolver.VARARGS_ARITY) String[] regex) {

    StringBuilder ob = new StringBuilder();

    if (!recordStoreController.tableExist(storeName)) {
      return ob.append("Store or alias ").append(storeName).append(" does not exist").toString();
    }

    ConsumerRecordTable store = recordStoreController.getTable(storeName);

    List<Pattern> patterns = new ArrayList<>();
    for (String pattern : regex) {
      patterns.add(Pattern.compile(pattern));
    }

    ob.append("Results for regex set '").append(Arrays.toString(regex)).append("':\n");
    Instant startTime = Instant.now();

    List<Pair<DbRecordKey, DbRecord>> results = new ArrayList<>();
    Predicate<Pair<DbRecordKey, DbRecord>> predicate = predicateFromRegexPatterns(patterns);
    store.doWith(predicate, results::add);
    results.sort(Comparator.comparing(o -> o.getValue().getRecordTimestamp().get()));
    Instant endTime = Instant.now();

    results.forEach(entry -> ob
        .append(readableTimestamp(entry.getValue().getRecordTimestamp().get())).append(": ")
        .append(DbRecordStringUtil.recordToString(entry))
        .append("\n"));

    ob.append("\n").append(results.size()).append(" Results found in ")
        .append(readableDuration(startTime, endTime)).append("\n");

    return ob.toString();
  }

  private String readableTimestamp(long timestamp) {
    return String.format("%-24s", Instant.ofEpochMilli(timestamp));
  }

  private Predicate<Pair<DbRecordKey, DbRecord>> predicateFromRegexPatterns(
      List<Pattern> patterns) {
    return pair -> {
      for (Pattern pattern : patterns) {
        if (!pattern.matcher(DbRecordStringUtil.recordToString(pair)).find()) {
          return false;
        }
      }
      return true;
    };
  }

  private String readableDuration(Instant startTime, Instant endTime) {
    long duration = endTime.toEpochMilli() - startTime.toEpochMilli();
    if (duration > 1000 * 60) {
      return String.format("%.1fmin", (double) duration / 60000);
    } else if (duration > 1000) {
      return String.format("%.3fsec", (double) duration / 1000);
    } else {
      return String.format("%dms", duration);
    }
  }

  @ShellMethod("List entities")
  public String list(@ShellOption String entity,
      @ShellOption(arity = StandardWithVarargsResolver.VARARGS_ARITY) String[] regex)
      throws Exception {
    if (LIST_ENTITIES.containsKey(entity.toUpperCase())) {
      List<String> list = LIST_ENTITIES.get(entity.toUpperCase()).apply(adminClient);
      return list.stream().filter(new RegexPredicate(regex)).collect(Collectors.joining("\n"));
    }
    System.err.println(String.format("Unknown entity to list: %s", entity));
    return "";
  }

  @ShellMethod("Listen to a topic")
  public String listen(
      @ShellOption("--topic") String topicName,
      @ShellOption(value = "--compact", arity = 0) boolean compact,
      @ShellOption(defaultValue = "null", value = "--alias") String alias,
      @ShellOption(defaultValue = "AVRO", value = "--key-type") SerdeType keyType,
      @ShellOption(defaultValue = "AVRO", value = "--value-type") SerdeType valueType,
      @ShellOption(arity = StandardWithVarargsResolver.VARARGS_ARITY, value = "--regex-filter") String[] regexFilter)
      throws ExecutionException, InterruptedException {
    if (alias.equals("null")) {
      alias = null;
    }
    StringBuilder ob = new StringBuilder();
    try {
      if (!topicExist(topicName)) {
        System.err.println(String
            .format("Topic %s does not exist. Use 'list topic' to get all topics available",
                topicName));
        return "";
      }

      kafkaSerdeController.registerTopicTypes(topicName, keyType, valueType);
      String tableName = recordStoreController
          .tableName(topicName, compact, regexFilter.length > 0);
      ConsumerRecordTable store;
      if (regexFilter.length > 0) {
        store = recordStoreController
            .newTableWithFilter(tableName, compact, new RegexPredicate(regexFilter));
      } else {
        store = recordStoreController.newTable(tableName, compact);
      }

      consumerThreadController.addStoreAssignment(topicName, store);
      consumerThreadController.addTopicToAssignment(topicName);

      boolean createAlias = false;
      if (alias != null) {
        if (recordStoreController.tableExist(alias)) {
          ob.append("WARN: could not use alias ").append(alias)
              .append(" since a store exists with that name\n");
        } else {
          createAlias = true;
          recordStoreController.setAlias(store.getName(), alias);
        }
      }
      ob.append("Created store ").append(store.getName());
      if (createAlias) {
        ob.append(" with alias ").append(alias);
      }

      return ob.toString();
    } catch (UndeclaredThrowableException e) {
      System.err.println("Could not listen to topic: " + e.getCause().getMessage());
      return "";
    }
  }

  private boolean topicExist(String topicName) throws ExecutionException, InterruptedException {
    ListTopicsResult listTopicsResult = adminClient.listTopics();
    Set<String> topics = listTopicsResult.names().get();
    return topics.contains(topicName);
  }

}
