package org.galatea.kafka.starter.testing.conversion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.junit.Before;
import org.junit.Test;

public class ConversionUtilTest {

  private ConversionUtil conversionUtil;

  @Before
  public void setup() {
    conversionUtil = new ConversionUtil();
  }

  @Test
  public void convertFieldValueSuccess() {
    String fieldPath = "fieldName";
    String fieldValue = "v";
    String expected = "Vv";
    Map<String, Function<String, Object>> conversionMap = new HashMap<>();
    conversionMap.put(fieldPath, string -> string.toUpperCase() + string.toLowerCase());

    Object converted = ConversionUtil.convertFieldValue(fieldPath, fieldValue, conversionMap);

    assertEquals(expected, converted);
  }

  @Test(expected = IllegalArgumentException.class)
  public void convertFieldValue_ConversionNoExist() {
    String fieldPath = "fieldName";
    String fieldValue = "v";
    Map<String, Function<String, Object>> conversionMap = new HashMap<>();

    ConversionUtil.convertFieldValue(fieldPath, fieldValue, conversionMap);
    fail();
  }

  @Test
  public void hasFieldConversionMethod_True() {
    String fieldPath = "fieldName";
    Map<String, Function<String, Object>> conversionMap = new HashMap<>();
    conversionMap.put(fieldPath, string -> string.toUpperCase() + string.toLowerCase());

    assertTrue(ConversionUtil.hasFieldConversionMethod(fieldPath, conversionMap));
  }

  @Test
  public void hasFieldConversionMethod_False() {
    String fieldPath = "fieldName";

    assertFalse(ConversionUtil.hasFieldConversionMethod(fieldPath, Collections.emptyMap()));
  }

  @Test
  public void useExistingTypeConversion() {
    conversionUtil.registerTypeConversion(LocalDate.class, Pattern.compile("^\\d+$"),
        (s, m) -> LocalDate.ofEpochDay(Long.parseLong(s)));
    Object convertedDate = conversionUtil.maybeUseTypeConversion(LocalDate.class, "1");

    assertEquals(LocalDate.ofEpochDay(1), convertedDate);
  }

  @Test
  public void useExistingTypeConversion_NoMatchPattern() {
    String converterInput = "1 1";
    conversionUtil.registerTypeConversion(LocalDate.class, Pattern.compile("^\\d+$"),
        (s, m) -> LocalDate.ofEpochDay(Long.parseLong(s)));
    Object convertedDate = conversionUtil.maybeUseTypeConversion(LocalDate.class, converterInput);

    assertEquals(converterInput, convertedDate);
  }

  @Test
  public void useMissingTypeConversion() {
    String converterInput = "1";
    Object convertedDate = conversionUtil.maybeUseTypeConversion(LocalDate.class, converterInput);

    assertEquals(converterInput, convertedDate);
  }

}