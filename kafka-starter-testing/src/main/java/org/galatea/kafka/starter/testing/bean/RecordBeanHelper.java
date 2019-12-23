package org.galatea.kafka.starter.testing.bean;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KeyValue;
import org.galatea.kafka.starter.testing.TopicConfig;
import org.galatea.kafka.starter.testing.alias.AliasHelper;
import org.galatea.kafka.starter.testing.avro.AvroMessageUtil;
import org.galatea.kafka.starter.testing.conversion.ConversionHelper;
import org.galatea.kafka.starter.testing.editor.InstantEditor;
import org.galatea.kafka.starter.testing.editor.LocalDateEditor;
import org.galatea.kafka.starter.testing.editor.LocalTimeEditor;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

public class RecordBeanHelper {

  public static final String PREFIX_KEY = "KEY.";
  public static final String PREFIX_VALUE = "VALUE.";

  public static <K, V> KeyValue<K, V> createRecord(Map<String, String> fields,
      TopicConfig<K, V> topicConfig) throws Exception {

    Set<String> fieldsUsed = new HashSet<>();
    K key = RecordBeanHelper.createKey(fields, topicConfig, fieldsUsed, PREFIX_KEY, PREFIX_VALUE);
    V value = RecordBeanHelper
        .createValue(fields, topicConfig, fieldsUsed, PREFIX_KEY, PREFIX_VALUE);

    Map<String, String> expandedFieldMap = AliasHelper
        .expandAliasKeys(fields, topicConfig.getAliases());

    // verify all fields in map were used
    Set<String> unusedFields = new HashSet<>(expandedFieldMap.keySet());
    unusedFields.removeAll(fieldsUsed);
    if (!unusedFields.isEmpty()) {
      StringBuilder sb = new StringBuilder("Fields were not used in creation of key or value ");
      sb.append("[").append(key.getClass().getSimpleName()).append("|")
          .append(value.getClass().getSimpleName()).append("]");
      unusedFields.forEach(unusedField -> sb.append("\n\t").append(unusedField));
      throw new IllegalArgumentException(sb.toString());
    }
    return new KeyValue<>(key, value);
  }

  public static <K, V> KeyValue<K, V> copyRecordPropertiesIntoNew(Set<String> fieldsToCopy,
      KeyValue<K, V> originalRecord, TopicConfig<K, V> topicConfig, String keyPrefix,
      String valuePrefix) throws Exception {

    fieldsToCopy = AliasHelper.expandAliasKeys(fieldsToCopy, topicConfig.getAliases());

    Set<String> copiedProperties = new HashSet<>();
    K newKey = topicConfig.createKey();
    copyBeanProperties(fieldsToCopy, originalRecord.key, newKey, copiedProperties, keyPrefix,
        valuePrefix);

    V newValue = topicConfig.createValue();
    copyBeanProperties(fieldsToCopy, originalRecord.value, newValue, copiedProperties, valuePrefix,
        keyPrefix);

    return new KeyValue<>(newKey, newValue);
  }

  public static <T> void copyBeanProperties(Set<String> fieldsToCopy, T source, T destination,
      Set<String> copiedProperties, String maybeIncludePrefix, String alwaysExcludePrefix) {
    BeanWrapper sourceWrap = wrapBean(source);
    BeanWrapper destinationWrap = wrapBean(destination);

    for (String fieldToCopy : fieldsToCopy) {
      if (!fieldToCopy.toLowerCase().startsWith(alwaysExcludePrefix.toLowerCase())) {
        if (fieldToCopy.toLowerCase().startsWith(maybeIncludePrefix.toLowerCase())) {
          fieldToCopy = fieldToCopy.substring(maybeIncludePrefix.length());
        }
        if (sourceWrap.isReadableProperty(fieldToCopy)) {
          destinationWrap.setPropertyValue(fieldToCopy, sourceWrap.getPropertyValue(fieldToCopy));
          copiedProperties.add(fieldToCopy);
        }
      }
    }
  }

  public static <K, V> K createKey(Map<String, String> fields, TopicConfig<K, V> topicConfig,
      Set<String> fieldsUsed, String keyPrefix, String valuePrefix) throws Exception {

    K key = topicConfig.createKey();
    Map<String, Function<String, Object>> conversions = topicConfig.getConversions();
    Map<String, String> aliases = topicConfig.getAliases();
    K populatedKey = populateBeanWithValues(key, fields, conversions, aliases, fieldsUsed,
        keyPrefix, valuePrefix);

    if (Arrays.asList(populatedKey.getClass().getInterfaces()).contains(SpecificRecord.class)) {
      AvroMessageUtil.defaultUtil()
          .populateRequiredFieldsWithDefaults((SpecificRecord) populatedKey);
    }
    return populatedKey;
  }

  public static <K, V> V createValue(Map<String, String> fields, TopicConfig<K, V> topicConfig,
      Set<String> fieldsUsed, String keyPrefix, String valuePrefix) throws Exception {

    V key = topicConfig.createValue();
    Map<String, Function<String, Object>> conversions = topicConfig.getConversions();
    Map<String, String> aliases = topicConfig.getAliases();
    V populatedValue = populateBeanWithValues(key, fields, conversions, aliases, fieldsUsed,
        valuePrefix, keyPrefix);

    if (Arrays.asList(populatedValue.getClass().getInterfaces()).contains(SpecificRecord.class)) {
      AvroMessageUtil.defaultUtil()
          .populateRequiredFieldsWithDefaults((SpecificRecord) populatedValue);
    }
    return populatedValue;
  }

  public static <T> T populateBeanWithValues(T object, Map<String, String> fields,
      Map<String, Function<String, Object>> conversions, Map<String, String> aliases,
      Set<String> fieldsUsed, String maybeIncludePrefix, String alwaysExcludePrefix) {

    conversions = AliasHelper.expandAliasKeys(conversions, aliases);
    BeanWrapper wrappedObj = wrapBean(object);

    Map<String, String> fieldMapExpanded = AliasHelper.expandAliasKeys(fields, aliases);

    for (Entry<String, String> entry : fieldMapExpanded.entrySet()) {
      String fullFieldPath = entry.getKey();
      String fieldValue = entry.getValue();

      if (!fullFieldPath.toLowerCase().startsWith(alwaysExcludePrefix.toLowerCase())) {
        String fieldPathWithoutPrefix = fullFieldPath;
        if (fullFieldPath.toLowerCase().startsWith(maybeIncludePrefix.toLowerCase())) {
          fieldPathWithoutPrefix = fullFieldPath.substring(maybeIncludePrefix.length());
        }

        if (wrappedObj.isReadableProperty(fieldPathWithoutPrefix)) {
          wrappedObj.setPropertyValue(fieldPathWithoutPrefix,
              ConversionHelper.maybeConvert(fieldPathWithoutPrefix, fieldValue, conversions));
          fieldsUsed.add(fullFieldPath);
        }
      }

    }
    return (T) wrappedObj.getWrappedInstance();
  }

  private static BeanWrapper wrapBean(Object bean) {
    BeanWrapper wrapper = new BeanWrapperImpl(bean);
    wrapper.registerCustomEditor(LocalDate.class, new LocalDateEditor());
    wrapper.registerCustomEditor(LocalTime.class, new LocalTimeEditor());
    wrapper.registerCustomEditor(Instant.class, new InstantEditor());
    wrapper.setAutoGrowNestedPaths(true);
    return wrapper;
  }
}
