package org.galatea.kafka.starter.testing.bean;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
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

  /**
   * Create record with specified fields set, using provided configuration for specific conversions
   * and aliases.
   *
   * @param fields map of {@code (alias|fully-qualified-field-name) -> field-value-as-string}
   * @param topicConfig configuration containing any field-specific conversions and alias mappings
   * @return record with values assigned
   * @throws Exception upon exception thrown in {@link TopicConfig#createKey()} or {@link
   * TopicConfig#createValue()}
   */
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

  /**
   * Copy specified properties from source record into new record
   *
   * @param fieldsToCopy set of fields (aliases or fully-qualified) to copy from source object
   * @param originalRecord record to copy fields from
   * @param topicConfig configuration containing aliases to use
   * @param keyPrefix prefix on fully-qualified fields to indicate the property is key-specific
   * @param valuePrefix prefix on fully-qualified fields to indicate the property is value-specific
   * @return new record with only specified properties copied from originalRecord
   * @throws Exception upon exception thrown in {@link TopicConfig#createKey()} or {@link
   * TopicConfig#createValue()}
   */
  public static <K, V> KeyValue<K, V> copyRecordPropertiesIntoNew(Set<String> fieldsToCopy,
      KeyValue<K, V> originalRecord, TopicConfig<K, V> topicConfig, String keyPrefix,
      String valuePrefix) throws Exception {

    HashSet<String> uncopiedFields = new HashSet<>(fieldsToCopy);
    fieldsToCopy = AliasHelper.expandAliasKeys(fieldsToCopy, topicConfig.getAliases());

    Set<String> copiedProperties = new HashSet<>();
    K newKey = topicConfig.createKey();
    copyBeanProperties(fieldsToCopy, originalRecord.key, newKey, copiedProperties, keyPrefix,
        valuePrefix);

    V newValue = topicConfig.createValue();
    copyBeanProperties(fieldsToCopy, originalRecord.value, newValue, copiedProperties, valuePrefix,
        keyPrefix);

    uncopiedFields.removeAll(copiedProperties);
    if (!uncopiedFields.isEmpty()) {
      StringBuilder sb = new StringBuilder(
          "Fields were not used in property copy of record types: ");
      sb.append("[").append(originalRecord.key.getClass().getSimpleName()).append("|")
          .append(originalRecord.value.getClass().getSimpleName()).append("]");
      uncopiedFields.forEach(unusedField -> sb.append("\n\t").append(unusedField));
      throw new IllegalArgumentException(sb.toString());
    }

    return new KeyValue<>(newKey, newValue);
  }

  /**
   * Copy specified properties from source to destination bean.
   *
   * @param fieldsToCopy fully qualified field paths to copy
   * @param source bean to copy properties from
   * @param destination bean to copy properties into
   * @param copiedProperties set of fields used. Every field successfully copied will add the
   * fully-qualified-field-name to this set
   * @param maybeIncludePrefix prefix that may be in front of fully-qualified-field-names that will
   * need to be removed (if present) before copying the field.
   * @param alwaysExcludePrefix prefix that, if present on a fully-qualified-field-name, prevents
   * the inclusion of that field in the copy.
   */
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

  /**
   * Create a bean with provided fields populated, of the type Key from the {@link TopicConfig}
   * provided.
   *
   * @param fields fields to populate the new key with. Additional properties may be present without
   * error.
   * @param topicConfig topic configuration to be used in creation of the key bean
   * @param fieldsUsed set of fields used. Every field set in the new object will add the
   * fully-qualified-field-name to this set
   * @param keyPrefix field name prefix that, when found, needs to be removed from the
   * fully-qualified-field-name before attempting to assign property to bean.
   * @param valuePrefix field name prefix that, when found, given field will be ignored for this
   * bean.
   * @return key bean with properties assigned
   * @throws Exception upon {@link TopicConfig#createKey()} exception
   */
  public static <K, V> K createKey(Map<String, String> fields, TopicConfig<K, V> topicConfig,
      Set<String> fieldsUsed, String keyPrefix, String valuePrefix) throws Exception {

    Map<String, Function<String, Object>> conversions = topicConfig.getConversions();
    Map<String, String> aliases = topicConfig.getAliases();
    K populatedKey = createBeanWithValues(topicConfig.getCreateEmptyKey(), fields, conversions,
        aliases, fieldsUsed, keyPrefix, valuePrefix);

    if (Arrays.asList(populatedKey.getClass().getInterfaces()).contains(SpecificRecord.class)) {
      AvroMessageUtil.defaultUtil()
          .populateRequiredFieldsWithDefaults((SpecificRecord) populatedKey);
    }
    return populatedKey;
  }

  /**
   * Create a bean with provided fields populated, of the type Value from the {@link TopicConfig}
   * provided.
   *
   * @param fields fields to populate the new value with. Additional properties may be present
   * without error.
   * @param topicConfig topic configuration to be used in creation of value bean
   * @param fieldsUsed set of fields used. Every field set in new object will add the
   * fully-qualified-field-name to this set
   * @param keyPrefix field name prefix that, when found, given field will be ignored for this
   * bean.
   * @param valuePrefix field name prefix that, when found, needs to be removed from
   * fully-qualified-field-name before attempting to assign property to bean.
   * @return value bean with properties assigned
   * @throws Exception upon {@link TopicConfig#createValue()} exception
   */
  public static <K, V> V createValue(Map<String, String> fields, TopicConfig<K, V> topicConfig,
      Set<String> fieldsUsed, String keyPrefix, String valuePrefix) throws Exception {

    Map<String, Function<String, Object>> conversions = topicConfig.getConversions();
    Map<String, String> aliases = topicConfig.getAliases();
    V populatedValue = createBeanWithValues(topicConfig.getCreateEmptyValue(), fields,
        conversions, aliases, fieldsUsed, valuePrefix, keyPrefix);

    if (Arrays.asList(populatedValue.getClass().getInterfaces()).contains(SpecificRecord.class)) {
      AvroMessageUtil.defaultUtil()
          .populateRequiredFieldsWithDefaults((SpecificRecord) populatedValue);
    }
    return populatedValue;
  }

  /**
   * Populate bean with provided values, using provided aliases for substitution, and provided
   * conversions prior to assignment. Adds the fully-qualified field names to provided set of used
   * fields. Bean is updated in-line as well as being returned from this method
   *
   * @param createBeanMethod method to call to create new bean
   * @param fields map of {@code (fully-qualified-field-name|alias) -> field-value-as-string}
   * defining fields to be set in the java bean. Values are set using {@link BeanWrapperImpl} as
   * well as a few custom editors that are defined in {@link RecordBeanHelper#wrapBean(Object)}. If
   * bean does not contain a given value in this map, it will not throw errors.
   * @param conversions map of {@code (fully-qualified-field-name|alias) -> conversion-method}
   * defining conversions that need to be specially handled on a field-by-field basis.
   * @param aliases map of {@code alias -> fully-qualified-field-name} defining alias substitution
   * for field names. Fully-qualified-field-name may include prefix {@link
   * RecordBeanHelper#PREFIX_KEY} or {@link RecordBeanHelper#PREFIX_VALUE} if the field should only
   * set the value in either key or value but not both (if key and value have identically named
   * field).
   * @param fieldsUsed Set of fields that contains the fully-qualified-field-names of all the fields
   * utilized when setting field values in the bean. This set is updated in-place.
   * @param maybeIncludePrefix prefix that may be in front of fully-qualified-field-names that will
   * need to be removed (if present) before assigning the field.
   * @param alwaysExcludePrefix prefix that, if present on a fully-qualified-field-name, prevents
   * the inclusion of that field in the bean.
   * @return modified bean.
   */
  public static <T> T createBeanWithValues(Callable<T> createBeanMethod,
      Map<String, String> fields, Map<String, Function<String, Object>> conversions,
      Map<String, String> aliases, Set<String> fieldsUsed, String maybeIncludePrefix,
      String alwaysExcludePrefix)
      throws Exception {

    conversions = AliasHelper.expandAliasKeys(conversions, aliases);
    BeanWrapper wrappedObj = wrapBean(createBeanMethod.call());

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

  /**
   * wrap a java bean and add predefined Editors that add support for commonly used types.
   * Configures bean to allow for automatic growth when properties are set using a nested path.
   * e.g.
   * setting property "field1.subField1" = "subValue" on object<pre>
   *   {@code {field1: null}}
   * </pre> will create the object inside field1 and set the value of the sub-object:<pre>
   *   {@code {field1: {subField1: "subValue"}}}
   * </pre>
   */
  private static BeanWrapper wrapBean(Object bean) {
    BeanWrapper wrapper = new BeanWrapperImpl(bean);
    wrapper.registerCustomEditor(LocalDate.class, new LocalDateEditor());
    wrapper.registerCustomEditor(LocalTime.class, new LocalTimeEditor());
    wrapper.registerCustomEditor(Instant.class, new InstantEditor());
    wrapper.setAutoGrowNestedPaths(true);
    return wrapper;
  }
}
