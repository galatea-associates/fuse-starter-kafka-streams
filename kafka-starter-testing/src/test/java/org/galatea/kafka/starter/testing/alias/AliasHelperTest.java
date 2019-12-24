package org.galatea.kafka.starter.testing.alias;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class AliasHelperTest {

  @Test
  public void expandMap() {
    String alias1 = "alias1";
    String fullyQualified1 = "fully.qualified.field1";
    String value1 = "field.value1";
    String alias2 = "alias2";
    String fullyQualified2 = "fully.qualified.field2";
    String value2 = "field.value2";
    String fullyQualified3 = "fully.qualified.field3";
    String value3 = "field.value3";

    Map<String, String> aliasMap = new HashMap<>();
    aliasMap.put(alias1, fullyQualified1);
    aliasMap.put(alias2, fullyQualified2);

    Map<String, String> fieldMap = new HashMap<>();
    fieldMap.put(alias1, value1);
    fieldMap.put(alias2, value2);
    fieldMap.put(fullyQualified3, value3);

    Map<String, String> expanded = AliasHelper.expandAliasKeys(fieldMap, aliasMap);

    assertEquals(value1, expanded.get(fullyQualified1));
    assertEquals(value2, expanded.get(fullyQualified2));
    assertEquals(value3, expanded.get(fullyQualified3));
    assertNull(expanded.get(alias1));
    assertNull(expanded.get(alias2));
  }

  @Test(expected = IllegalArgumentException.class)
  public void expandMapWithAliasOverrideField() {
    String alias1 = "alias1";
    String fullyQualified = "fully.qualified.field1";
    String value1 = "field.value1";
    String value2 = "field.value2";

    Map<String, String> aliasMap = new HashMap<>();
    aliasMap.put(alias1, fullyQualified);

    Map<String, String> fieldMap = new HashMap<>();
    fieldMap.put(alias1, value1);
    fieldMap.put(fullyQualified, value2);

    Map<String, String> expandedMap = AliasHelper.expandAliasKeys(fieldMap, aliasMap);
    fail();
  }

  @Test
  public void expandSet() {
    String alias1 = "alias1";
    String fullyQualified1 = "fully.qualified.field1";
    String alias2 = "alias2";
    String fullyQualified2 = "fully.qualified.field2";
    String fullyQualified3 = "fully.qualified.field3";

    Map<String, String> aliasMap = new HashMap<>();
    aliasMap.put(alias1, fullyQualified1);
    aliasMap.put(alias2, fullyQualified2);

    Set<String> fieldSet = new HashSet<>();
    fieldSet.add(alias1);
    fieldSet.add(alias2);
    fieldSet.add(fullyQualified3);

    Set<String> expanded = AliasHelper.expandAliasKeys(fieldSet, aliasMap);

    assertTrue(expanded.contains(fullyQualified1));
    assertTrue(expanded.contains(fullyQualified2));
    assertTrue(expanded.contains(fullyQualified3));
    assertFalse(expanded.contains(alias1));
    assertFalse(expanded.contains(alias2));
  }
}