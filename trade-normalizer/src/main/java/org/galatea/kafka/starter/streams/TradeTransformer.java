package org.galatea.kafka.starter.streams;

import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.galatea.kafka.starter.messaging.streams.TaskContext;
import org.galatea.kafka.starter.messaging.streams.TaskStoreRef;
import org.galatea.kafka.starter.messaging.streams.TransformerRef;
import org.galatea.kafka.starter.messaging.trade.TradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.TradeMsgValue;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgValue;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TradeTransformer implements
    TransformerRef<InputTradeMsgKey, InputTradeMsgValue, TradeMsgKey, TradeMsgValue, Object> {

  @Override
  public KeyValue<TradeMsgKey, TradeMsgValue> transform(InputTradeMsgKey key,
      InputTradeMsgValue value, TaskContext<Object> context) {
    log.info("Processing {} | {}", key, value);
    return null;
  }

  @Override
  public Collection<TaskStoreRef<?, ?>> taskStores() {
    return null;
  }
}
