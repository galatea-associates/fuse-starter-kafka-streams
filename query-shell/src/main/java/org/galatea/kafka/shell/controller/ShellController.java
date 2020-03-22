package org.galatea.kafka.shell.controller;

import java.lang.reflect.UndeclaredThrowableException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.galatea.kafka.shell.config.MessagingConfig;
import org.galatea.kafka.shell.config.StandardWithVarargsResolver;
import org.galatea.kafka.shell.consumer.ConsumerThreadController;
import org.galatea.kafka.shell.domain.DbRecord;
import org.galatea.kafka.shell.domain.DbRecordKey;
import org.galatea.kafka.shell.stores.ConsumerRecordTable;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

@Slf4j
@ShellComponent
public class ShellController {

  private final RecordStoreController recordStoreController;
  private final ConsumerThreadController consumerThreadController;
  private final StatusController statusController;

  public ShellController(RecordStoreController recordStoreController,
      ConsumerThreadController consumerThreadController, StatusController statusController,
      MessagingConfig messagingConfig) {
    this.recordStoreController = recordStoreController;
    this.consumerThreadController = consumerThreadController;
    this.statusController = statusController;

    System.out
        .println(String.format("Connected to brokers: %s", messagingConfig.getBootstrapServer()));
    System.out.println(
        String.format("Connected to schema registry: %s", messagingConfig.getSchemaRegistryUrl()));
  }

  // TODO: live-updating status
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

    results.forEach(entry -> ob.append(Instant.ofEpochMilli(entry.value.getRecordTimestamp().get()))
        .append(": ").append(entry.value.getStringValue()).append("\n"));

    ob.append("\n").append(results.size()).append(" Results found in ")
        .append(readableDuration(startTime, endTime)).append("\n");

    return ob.toString();
  }

  private Predicate<KeyValue<DbRecordKey, DbRecord>> predicateFromRegexPatterns(List<Pattern> patterns) {
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

  @ShellMethod("Listen to a topic")
  public String listen(
      @ShellOption String topicName,
      @ShellOption String compact,
      @ShellOption(defaultValue = "null") String alias)
      throws ExecutionException, InterruptedException {
    if (alias.equals("null")) {
      alias = null;
    }

    StringBuilder ob = new StringBuilder();
    try {
      boolean effectiveCompact = Boolean.parseBoolean(compact);
      if (recordStoreController.tableExist(topicName, effectiveCompact)) {
        return ob.append("Already listening to topic ").append(topicName)
            .append(" with config compact=").append(effectiveCompact).toString();
      }
      ConsumerRecordTable store = recordStoreController.newTable(topicName, effectiveCompact);
      consumerThreadController.addStoreAssignment(topicName, store);
      // TODO: check if topic exists before trying to add
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

}
