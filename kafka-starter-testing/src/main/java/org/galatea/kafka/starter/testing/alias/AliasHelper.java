package org.galatea.kafka.starter.testing.alias;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class AliasHelper {

  public static <T> Map<String, T> expandAliasKeys(Map<String, T> fieldMap,
      Map<String, String> aliasMap) {
    Map<String, T> outputMap = new HashMap<>(fieldMap);

    Set<String> keysToRemove = new HashSet<>();
    Map<String, T> entriesToAdd = new HashMap<>();
    for (Entry<String, String> entry : aliasMap.entrySet()) {
      String alias = entry.getKey();
      String fullFieldName = entry.getValue();
      if (outputMap.containsKey(alias)) {
        keysToRemove.add(alias);
        entriesToAdd.put(fullFieldName, outputMap.get(alias));

        if (outputMap.containsKey(fullFieldName)) {
          throw new IllegalArgumentException(String
              .format("Alias substitution cannot override existing entry for alias %s=>%s", alias,
                  fullFieldName));
        }
      }
    }
    keysToRemove.forEach(outputMap::remove);
    outputMap.putAll(entriesToAdd);

    return outputMap;
  }

  public static Set<String> expandAliasKeys(Set<String> fieldSet,
      Map<String, String> aliasMap) {
    Set<String> outputSet = new HashSet<>(fieldSet);

    Set<String> entriesToRemove = new HashSet<>();
    Set<String> entriesToAdd = new HashSet<>();

    for (Entry<String, String> entry : aliasMap.entrySet()) {
      String alias = entry.getKey();
      String fullFieldName = entry.getValue();
      if (outputSet.contains(alias)) {
        entriesToRemove.add(alias);
        entriesToAdd.add(fullFieldName);
      }
    }
    entriesToRemove.forEach(outputSet::remove);
    outputSet.addAll(entriesToAdd);

    return outputSet;
  }
}
