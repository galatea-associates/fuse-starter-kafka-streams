package org.galatea.kafka.shell.controller;

import java.lang.reflect.UndeclaredThrowableException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.apache.kafka.streams.KeyValue;
import org.apache.logging.log4j.util.Strings;
import org.galatea.kafka.shell.config.MessagingConfig;
import org.galatea.kafka.shell.config.StandardWithVarargsResolver;
import org.galatea.kafka.shell.domain.DbRecord;
import org.galatea.kafka.shell.domain.DbRecordKey;
import org.galatea.kafka.shell.domain.SerdeType;
import org.galatea.kafka.shell.stores.ConsumerRecordTable;
import org.galatea.kafka.shell.util.ListEntityFunction;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

@Slf4j
@ShellComponent
public class ShellController {

  // TODO: Additional details in 'status' command such as table alias
  // TODO: add ability to create tables that are filtered on receiving messages

  private final RecordStoreController recordStoreController;
  private final ConsumerThreadController consumerThreadController;
  private final StatusController statusController;
  private final AdminClient adminClient;
  private final KafkaSerdeController kafkaSerdeController;
  private final static Map<String, ListEntityFunction> LIST_ENTITIES = new HashMap<>();

  static {
    LIST_ENTITIES.put("TOPIC", client -> Strings.join(client.listTopics().names().get(), '\n'));
    LIST_ENTITIES
        .put("CONSUMER-GROUP", client -> client.listConsumerGroups().all().get().stream().map(
            ConsumerGroupListing::groupId).collect(Collectors.joining("\n")));
  }

  public ShellController(RecordStoreController recordStoreController,
      ConsumerThreadController consumerThreadController, StatusController statusController,
      MessagingConfig messagingConfig, AdminClient adminClient,
      KafkaSerdeController kafkaSerdeController) {
    this.recordStoreController = recordStoreController;
    this.consumerThreadController = consumerThreadController;
    this.statusController = statusController;
    this.adminClient = adminClient;
    this.kafkaSerdeController = kafkaSerdeController;

    System.out
        .println(String.format("Connected to brokers: %s", messagingConfig.getBootstrapServer()));
    System.out.println(
        String.format("Connected to schema registry: %s", messagingConfig.getSchemaRegistryUrl()));
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

  @ShellMethod("Get status of the service")
  public String status() throws InterruptedException {
    return statusController.printableStatus();
  }

  @ShellMethod("Search a store using REGEX")
  public String query(
      @ShellOption String storeName,
      @ShellOption(arity = StandardWithVarargsResolver.VARARGS_ARITY, defaultValue = "NONE") String[] regex) {

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

    Collection<KeyValue<DbRecordKey, DbRecord>> results = new ArrayList<>();
    Predicate<KeyValue<DbRecordKey, DbRecord>> predicate = predicateFromRegexPatterns(patterns);
    store.doWith(predicate, results::add);
    Instant endTime = Instant.now();

    results.forEach(entry -> ob
        .append(readableTimestamp(entry.value.getRecordTimestamp().get())).append(":")
        .append(" Key: ").append(entry.key.getByteKey())
        .append(" Value: ").append(entry.value.getStringValue().get())
        .append("\n"));

    ob.append("\n").append(results.size()).append(" Results found in ")
        .append(readableDuration(startTime, endTime)).append("\n");

    return ob.toString();
  }

  private String readableTimestamp(long timestamp) {
    return String.format("%-24s", Instant.ofEpochMilli(timestamp));
  }

  private Predicate<KeyValue<DbRecordKey, DbRecord>> predicateFromRegexPatterns(
      List<Pattern> patterns) {
    return dbRecord -> {
      for (Pattern pattern : patterns) {
        if (!pattern.matcher(dbRecord.value.getStringValue().get()).find()) {
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
  public String list(@ShellOption String entity) throws Exception {
    if (LIST_ENTITIES.containsKey(entity.toUpperCase())) {
      return LIST_ENTITIES.get(entity.toUpperCase()).apply(adminClient);
    }
    System.err.println(String.format("Unknown entity to list: %s", entity));
    return "";
  }

  @ShellMethod("Listen to a topic")
  public String listen(
      @ShellOption String topicName,
      @ShellOption String compact,
      @ShellOption(defaultValue = "null") String alias,
      @ShellOption(defaultValue = "AVRO") String keyType,
      @ShellOption(defaultValue = "AVRO") String valueType)
      throws ExecutionException, InterruptedException {
    if (alias.equals("null")) {
      alias = null;
    }

    StringBuilder ob = new StringBuilder();
    try {
      boolean effectiveCompact = Boolean.parseBoolean(compact);
      if (recordStoreController.tableExist(topicName, effectiveCompact)) {
        System.err.println(String
            .format("Already listening to topic %s with config compact=%s", topicName,
                effectiveCompact));
        return "";
      }

      if (!topicExist(topicName)) {
        System.err.println(String
            .format("Topic %s does not exist. Use 'list topic' to get all topics available",
                topicName));
        return "";
      }

      if (!kafkaSerdeController.isValidType(keyType)) {
        System.err.println(String.format("Key Type %s is not a configured type. Options: %s",
            keyType, kafkaSerdeController.validTypes()));
        return "";
      }

      if (!kafkaSerdeController.isValidType(valueType)) {
        System.err.println(String.format("Value Type %s is not a configured type. Options: %s",
            valueType, kafkaSerdeController.validTypes()));
        return "";
      }

      kafkaSerdeController
          .registerTopicTypes(topicName, SerdeType.valueOf(keyType), SerdeType.valueOf(valueType));
      ConsumerRecordTable store = recordStoreController.newTable(topicName, effectiveCompact);
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
