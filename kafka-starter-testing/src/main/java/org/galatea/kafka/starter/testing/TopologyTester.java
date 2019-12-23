package org.galatea.kafka.starter.testing;

import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.galatea.kafka.starter.messaging.test.TestMsg;
import org.galatea.kafka.starter.messaging.trade.TradeMsgKey;
import org.galatea.kafka.starter.testing.bean.RecordBeanHelper;

@Slf4j
public class TopologyTester {


  public void test() throws Exception {

    TopicConfig<TradeMsgKey, TestMsg> topicConfig = new TopicConfig<>(TradeMsgKey::new,
        TestMsg::new);

    Map<String, String> fieldMap = new HashMap<>();
    fieldMap.put("tradeId", "100d");
    fieldMap.put("value.nonNullableTimestamp", "2019-12-20T10:43:00.000Z");
    fieldMap.put("nonNullableDate", "2019-12-20");

    KeyValue<TradeMsgKey, TestMsg> record = RecordBeanHelper.createRecord(fieldMap, topicConfig);
    log.info("Record: {}", record);
  }


}
