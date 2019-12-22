package org.galatea.kafka.starter.testing;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema.Type;
import org.galatea.kafka.starter.messaging.test.TestMsg;
import org.galatea.kafka.starter.testing.avro.AvroMessageUtil;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroArrayType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroBooleanType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroBytesType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroDoubleType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroEnumType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroFixedType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroFloatType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroIntType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroLocalDateType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroLocalTimeType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroLongType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroMapType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroStringType;
import org.galatea.kafka.starter.testing.avro.fieldtypes.AvroTimestampType;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

@Slf4j
public class TopologyTester {

  public void test() throws InstantiationException, IllegalAccessException, ClassNotFoundException {

    TestMsg testMsg = new TestMsg();
    BeanWrapper wrapper = new BeanWrapperImpl(testMsg);
    wrapper.setAutoGrowNestedPaths(true);

    wrapper.setPropertyValue("nullableMapField[index_1].nonNullableEnum", "ENUM_VALUE2");

    AvroMessageUtil.defaultUtil().populateRequiredFieldsWithDefaults(testMsg);
    log.info("TestMsg: {}", testMsg);
  }

}
