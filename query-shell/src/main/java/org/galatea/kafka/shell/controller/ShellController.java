package org.galatea.kafka.shell.controller;

import com.apple.foundationdb.tuple.Tuple;
import java.lang.reflect.UndeclaredThrowableException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.galatea.kafka.shell.config.MessagingConfig;
import org.galatea.kafka.shell.consumer.ConsumerThreadController;
import org.galatea.kafka.shell.stores.OffsetTrackingRecordStore;
import org.galatea.kafka.shell.util.Extractor;
import org.rocksdb.RocksIterator;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

@Slf4j
@ShellComponent
public class ShellController {

  private final Map<String, String> storeAlias = new HashMap<>();
  private final RecordStoreController recordStoreController;
  private final ConsumerThreadController consumerThreadController;
  private final StatusController statusController;
  private final MessagingConfig messagingConfig;

  public ShellController(RecordStoreController recordStoreController,
      ConsumerThreadController consumerThreadController, StatusController statusController,
      MessagingConfig messagingConfig) {
    this.recordStoreController = recordStoreController;
    this.consumerThreadController = consumerThreadController;
    this.statusController = statusController;
    this.messagingConfig = messagingConfig;
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
      @ShellOption(valueProvider = Extractor.class) String regex) {

    StringBuilder ob = new StringBuilder();
    if (!recordStoreController.storeExist(storeName) && !storeAlias.containsKey(storeName)) {
      return ob.append("Store or alias ").append(storeName).append(" does not exist").toString();
    } else if (!recordStoreController.storeExist(storeName)) {
      storeName = storeAlias.get(storeName);
    }

    OffsetTrackingRecordStore episodeStore = recordStoreController.getStores().get(storeName);

    RocksIterator iterator = episodeStore.getUnderlyingDb().newIterator();
    iterator.seekToFirst();

    List<Pattern> patterns = new ArrayList<>();
    for (String pattern : regex) {
      patterns.add(Pattern.compile(pattern));
    }

    ob.append("Results for regex set '").append(Arrays.toString(regex)).append("':\n");
    Instant startTime = Instant.now();
    long numResults = 0;

    while (iterator.isValid()) {
      Tuple valueTuple = Tuple.fromBytes(iterator.value());
      boolean recordMatches = true;
      String recordString = valueTuple.getString(3);
      for (Pattern pattern : patterns) {
        if (!pattern.matcher(recordString).find()) {
          recordMatches = false;
          break;
        }
      }

      if (recordMatches) {
        ob.append(Instant.ofEpochMilli(valueTuple.getLong(2)))
            .append(": ").append(recordString).append("\n");
      }
      iterator.next();
    }

    ob.append("\n").append(numResults).append(" Results found in ")
        .append(readableTimeSince(startTime)).append("\n");
    iterator.close();

    // invoke service
    return ob.toString();
  }

  private String readableTimeSince(Instant startTime) {
    long duration = Instant.now().toEpochMilli() - startTime.toEpochMilli();
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
      if (recordStoreController.storeExist(topicName, effectiveCompact)) {
        return ob.append("Already listening to topic ").append(topicName)
            .append(" with config compact=").append(effectiveCompact).toString();
      }
      OffsetTrackingRecordStore store = recordStoreController.newStore(topicName, effectiveCompact);
      consumerThreadController.addStoreAssignment(topicName, store);
      consumerThreadController.addTopicToAssignment(topicName);

      boolean createAlias = false;
      if (alias != null) {
        if (recordStoreController.storeExist(alias)) {
          ob.append("WARN: could not use alias ").append(alias)
              .append(" since a store exists with that name\n");
        } else {
          createAlias = true;
          storeAlias.put(alias, store.getStoreName());
        }
      }
      ob.append("Created store ").append(store.getStoreName());
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
