package org.galatea.kafka.shell.controller;

import com.github.fonimus.ssh.shell.commands.SshShellComponent;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
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
import org.galatea.kafka.shell.domain.SerdeType.DataType;
import org.galatea.kafka.shell.domain.ShellEntityType;
import org.galatea.kafka.shell.stores.ConsumerRecordTable;
import org.galatea.kafka.shell.util.DbRecordStringUtil;
import org.galatea.kafka.shell.util.RegexPredicate;
import org.galatea.kafka.starter.util.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

@Slf4j
@SshShellComponent
public class ShellController {

  // TODO: add separate consumer thread per topic
  // TODO: add consumer group consumption rate (per partition, summed for per-topic and total)
  // TODO: figure out how to parse ShellEntityType without requiring caps, and replacing "-" with "_"
  private static final String REGEX_HELP = "search filter regex. Optional. more than 1 argument "
      + "will be used as additional filters resulting in regex1 AND regex2 AND ... ";

  private final RecordStoreController recordStoreController;
  private final ConsumerThreadController consumerThreadController;
  private final StatusController statusController;
  private final AdminClient adminClient;
  private final SchemaRegistryController schemaRegistryController;
  private final KafkaSerdeController kafkaSerdeController;
  private final MessagingConfig messagingConfig;
  private boolean connected = false;
  @Value("${shell.query.max-results}")
  private long maxQueryResults;
  @Value("${shell.store.reset-all-cron}")
  private String resetStoresCron;

  public ShellController(RecordStoreController recordStoreController,
      ConsumerThreadController consumerThreadController, StatusController statusController,
      MessagingConfig messagingConfig, AdminClient adminClient,
      SchemaRegistryController schemaRegistryController,
      KafkaSerdeController kafkaSerdeController,
      TaskScheduler scheduler) throws Exception {
    this.recordStoreController = recordStoreController;
    this.consumerThreadController = consumerThreadController;
    this.statusController = statusController;
    this.adminClient = adminClient;
    this.schemaRegistryController = schemaRegistryController;
    this.kafkaSerdeController = kafkaSerdeController;
    this.messagingConfig = messagingConfig;

    if (resetStoresCron != null) {
      scheduler.schedule(() -> recordStoreController.getTables().keySet().forEach(this::stopListen),
          new CronTrigger(resetStoresCron));
    }

    System.out.println(getConnectionStatus(messagingConfig, adminClient));
    if (!connected) {
      System.exit(1);
    }
  }

  private String getConnectionStatus(MessagingConfig messagingConfig, AdminClient adminClient)
      throws InterruptedException {
    StringBuilder sb = new StringBuilder();
    try {
      String clusterId = adminClient.describeCluster().clusterId().get();
      if (messagingConfig.getEnvironmentId() != null) {
        sb.append(
            String.format("Environment: %s\n", messagingConfig.getEnvironmentId()));
      }
      sb.append(String.format("Connected to brokers: %s\n", messagingConfig.getBootstrapServer()));
      sb.append(String
          .format("Connected to schema registry: %s\n", messagingConfig.getSchemaRegistryUrl()));
      sb.append(String.format("Cluster ID: %s\n", clusterId));
      connected = true;
    } catch (ExecutionException e) {
      sb.append(
          String.format("Could not connect to brokers %s\n", messagingConfig.getBootstrapServer()));
      connected = false;
    }
    return sb.toString();
  }

  @ShellMethod(value = "Stop listening to a topic and remove subscribed stores", key = "stop-listen")
  public String stopListen(@ShellOption String topicName) {

    Set<ConsumerRecordTable> subscribedTables = consumerThreadController
        .removeTopicAssignment(topicName);
    subscribedTables.forEach(table -> recordStoreController.deleteTable(table.getName()));

    return String.format("Stopped consuming topic %s and deleted stores %s", topicName,
        subscribedTables.stream().map(ConsumerRecordTable::getName)
            .collect(Collectors.joining(", ")));
  }

  @ShellMethod("Get details about an entity")
  public String describe(@ShellOption ShellEntityType entityType, @ShellOption boolean detail,
      @ShellOption String name,
      /* DO NOT put arguments after an option that uses VARARGS since VARARGS will use all remaining arguments*/
      @ShellOption(arity = StandardWithVarargsResolver.VARARGS_ARITY) String[] parameters)
      throws ExecutionException, InterruptedException, IOException, RestClientException {
    return statusController.printableDetails(entityType, name, detail, parameters);
  }

  @ShellMethod("Get status of the service")
  public String status() throws InterruptedException {
    return getConnectionStatus(messagingConfig, adminClient) + "\n" + statusController.printableStatus();
  }

  @ShellMethod("Search a store using REGEX")
  public String query(@ShellOption String storeName, @ShellOption boolean detail,
      @ShellOption boolean all,
      /* DO NOT put arguments after an option that uses VARARGS since VARARGS will use all remaining arguments*/
      @ShellOption(help = REGEX_HELP, arity = StandardWithVarargsResolver.VARARGS_ARITY) String[] regexFilter) {

    StringBuilder ob = new StringBuilder();

    if (!recordStoreController.tableExist(storeName)) {
      return ob.append("Store or alias ").append(storeName).append(" does not exist").toString();
    }

    ConsumerRecordTable store = recordStoreController.getTable(storeName);

    List<Pattern> patterns = new ArrayList<>();
    for (String pattern : regexFilter) {
      patterns.add(Pattern.compile(pattern));
    }

    ob.append("Results for regex set '").append(Arrays.toString(regexFilter)).append("':\n");
    ob.append("  (Record Timestamp");
    if (detail) {
      ob.append(" [partition-offset]");
    }
    ob.append(": Key, Value)\n");
    Instant startTime = Instant.now();

    List<Pair<DbRecordKey, DbRecord>> results = new ArrayList<>();
    Predicate<Pair<DbRecordKey, DbRecord>> predicate = predicateFromRegexPatterns(patterns);
    store.doWith(predicate, results::add, all ? Long.MAX_VALUE : maxQueryResults);
    results.sort(Comparator.comparing(o -> o.getValue().getRecordTimestamp().get()));
    Instant endTime = Instant.now();

    long recordCount = 0;
    for (Pair<DbRecordKey, DbRecord> entry : results) {
      ob.append(readableTimestamp(entry.getValue().getRecordTimestamp().get()));
      if (detail) {
        ob.append(" [").append(entry.getValue().getPartition()).append("-")
            .append(entry.getValue().getOffset()).append("]");
      }
      ob.append(": ").append(DbRecordStringUtil.recordToString(entry)).append("\n");

      if (!all && ++recordCount >= maxQueryResults) {
        ob.append("\nResults limited to max-query-results (").append(maxQueryResults)
            .append("); use --all to get more");
        break;
      }
    }

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
      if (pair.getValue().getStringValue() == null) {
        return false;
      }
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

  @ShellMethod("List entities. Sorted alphabetically. Potential entities: Topic, Consumer-group, Schema")
  public String list(@ShellOption(help = "TOPIC, CONSUMER-GROUP, SCHEMA") String entity,
      /* DO NOT put arguments after an option that uses VARARGS since VARARGS will use all remaining arguments*/
      @ShellOption(help = REGEX_HELP, arity = StandardWithVarargsResolver.VARARGS_ARITY) String[] regexFilter)
      throws Exception {

    entity = entity.toUpperCase();
    List<String> entries;
    switch (entity) {
      case "TOPIC":
        entries = new ArrayList<>(adminClient.listTopics().names().get());
        break;
      case "CONSUMER-GROUP":
        entries = adminClient.listConsumerGroups().all().get().stream()
            .map(ConsumerGroupListing::groupId).collect(Collectors.toList());
        break;
      case "SCHEMA":
        entries = schemaRegistryController.listSubjects();
        break;
      default:
        return "Unconfigured entity " + entity;
    }

    return entries.stream().sorted().filter(new RegexPredicate(regexFilter))
        .collect(Collectors.joining("\n"));
  }

  @ShellMethod("Listen to a topic and create store that will consume it")
  public String listen(
      @ShellOption("--topic") String topicName,
      @ShellOption boolean compact,
      @ShellOption(defaultValue = "null") String alias,
      @ShellOption(help = "Potential Values: TUPLE, AVRO, LONG, INTEGER, SHORT, FLOAT, DOUBLE, "
          + "STRING, BYTEBUFFER, BYTES, BYTE_ARRAY", defaultValue = "AVRO") DataType keyType,
      @ShellOption(help = "Potential Values: TUPLE, AVRO, LONG, INTEGER, SHORT, FLOAT, DOUBLE, "
          + "STRING, BYTEBUFFER, BYTES, BYTE_ARRAY", defaultValue = "AVRO") DataType valueType,
      /* DO NOT put arguments after an option that uses VARARGS since VARARGS will use all remaining arguments*/
      @ShellOption(help = REGEX_HELP, arity = StandardWithVarargsResolver.VARARGS_ARITY) String[] regexFilter)
      throws ExecutionException, InterruptedException {
    if (alias.equals("null")) {
      alias = null;
    }
    StringBuilder ob = new StringBuilder();

    try {
      if (!topicExist(topicName)) {
        return String.format("Topic %s does not exist. Use 'list topic' to get all topics "
            + "available", topicName);
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
      return "Could not listen to topic: " + e.getCause().getMessage();
    }
  }

  private boolean topicExist(String topicName) throws ExecutionException, InterruptedException {
    ListTopicsResult listTopicsResult = adminClient.listTopics();
    Set<String> topics = listTopicsResult.names().get();
    return topics.contains(topicName);
  }

}
