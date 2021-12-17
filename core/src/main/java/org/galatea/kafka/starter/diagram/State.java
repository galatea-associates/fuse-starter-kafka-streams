package org.galatea.kafka.starter.diagram;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class State {

  public static final String INPUT_STORE = "input";
  public static final String OUTPUT_STORE = "output";
  public static final String INTERMEDIATE_STORE = "intermediate";
  public static final String SYSTEM_STORE = "sys";

  private final Participant participant;
  private final UmlBuilder builder;
  private final Map<Object, Object> innerState = new LinkedHashMap<>();

  State(Participant participant, UmlBuilder builder) {
    this.participant = participant;
    this.builder = builder;
  }

  public void put(KvPair kvPair) {
    put(kvPair.getKey(), Optional.ofNullable(kvPair.getValue()).map(Objects::toString).orElse(null));
  }

  public void put(String storeType, KvPair kvPair) {
    String value = Optional.ofNullable(kvPair.getValue()).map(Object::toString).orElse(null);
    put(String.format("(%s) %s", storeType, kvPair.getKey()), value);
  }

  public void put(Object key, Object value) {
    if (value == null) {
      innerState.remove(key);
    } else {
      innerState.put(key, value);
    }
  }

  public void logState() {
    logState(false);
  }

  public void logState(boolean inlineWithLastNote) {

    String stateString = innerState.entrySet().stream()
        .sorted(Comparator.comparing(e -> String.valueOf(e.getKey()), storeEntryComparator))
        .map(e -> String.format("%s: %s", e.getKey(), e.getValue()))
        .collect(Collectors.joining("\n  "));
    builder.addNote(participant, "State:\n  " + stateString, inlineWithLastNote);
  }
  
  public void remove(String storeType, KvPair pair) {
    put(storeType, pair.withValue(null));
  }

  public static final Comparator<String> storeEntryComparator = (o1, o2) -> {
    String inputStorePrefix = storeEntryPrefix(INPUT_STORE);
    String outputStorePrefix = storeEntryPrefix(OUTPUT_STORE);
    String intermediateStorePrefix = storeEntryPrefix(INTERMEDIATE_STORE);
    String systemPrefix = storeEntryPrefix(SYSTEM_STORE);
    if (o1.startsWith(systemPrefix) && !o2.startsWith(systemPrefix)) {
      return -1;
    } else if (!o1.startsWith(systemPrefix) && o2.startsWith(systemPrefix)) {
      return 1;
    }
    if (o1.startsWith(inputStorePrefix) && !o2.startsWith(inputStorePrefix)) {
      return -1;
    } else if (!o1.startsWith(inputStorePrefix) && o2.startsWith(inputStorePrefix)) {
      return 1;
    }
    if (o1.startsWith(intermediateStorePrefix) && !o2.startsWith(intermediateStorePrefix)) {
      return -1;
    } else if (!o1.startsWith(intermediateStorePrefix) && o2.startsWith(intermediateStorePrefix)) {
      return 1;
    }
    if (o1.startsWith(outputStorePrefix) && !o2.startsWith(outputStorePrefix)) {
      return -1;
    } else if (!o1.startsWith(outputStorePrefix) && o2.startsWith(outputStorePrefix)) {
      return 1;
    }

    o1 = o1.replaceFirst(inputStorePrefix, "")
        .replaceFirst(outputStorePrefix, "")
        .replaceFirst(intermediateStorePrefix, "")
        .replaceFirst(outputStorePrefix, "");
    o2 = o2.replaceFirst(inputStorePrefix, "")
        .replaceFirst(outputStorePrefix, "")
        .replaceFirst(intermediateStorePrefix, "")
        .replaceFirst(outputStorePrefix, "");
    return Comparator.comparing(Objects::toString).compare(o1, o2);
  };

  private static String storeEntryPrefix(String storeType) {
    return "(" + storeType + ")";
  }
}
