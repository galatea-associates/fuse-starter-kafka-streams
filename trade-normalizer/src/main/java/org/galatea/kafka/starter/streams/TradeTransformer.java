package org.galatea.kafka.starter.streams;

import java.util.Collection;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.galatea.kafka.starter.messaging.security.SecurityIsinMsgKey;
import org.galatea.kafka.starter.messaging.security.SecurityMsgValue;
import org.galatea.kafka.starter.messaging.streams.GlobalStore;
import org.galatea.kafka.starter.messaging.streams.GlobalStoreRef;
import org.galatea.kafka.starter.messaging.streams.ProcessorTaskContext;
import org.galatea.kafka.starter.messaging.streams.TaskStore;
import org.galatea.kafka.starter.messaging.streams.TaskStoreRef;
import org.galatea.kafka.starter.messaging.streams.TransformerRef;
import org.galatea.kafka.starter.messaging.streams.annotate.PunctuateMethod;
import org.galatea.kafka.starter.messaging.trade.TradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.TradeMsgValue;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgValue;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TradeTransformer extends
    TransformerRef<InputTradeMsgKey, InputTradeMsgValue, TradeMsgKey, TradeMsgValue> {

  private final GlobalStoreRef<SecurityIsinMsgKey, SecurityMsgValue> securityStoreRef;
  private final TaskStoreRef<InputTradeMsgKey, InputTradeMsgValue> tradeStoreRef;

  @Override
  public KeyValue<TradeMsgKey, TradeMsgValue> transform(InputTradeMsgKey key,
      InputTradeMsgValue value, ProcessorTaskContext<TradeMsgKey, TradeMsgValue, Object> context) {
    log.info("{} Processing {} | {}", context.taskId(), key, value);
    GlobalStore<SecurityIsinMsgKey, SecurityMsgValue> securityStore = context
        .store(securityStoreRef);
    TaskStore<InputTradeMsgKey, InputTradeMsgValue> store = context.store(tradeStoreRef);
    Optional<InputTradeMsgValue> existingTrade = store.get(key);
    existingTrade.ifPresent(trade -> log.info("Existing trade: {}", trade));
    store.put(key, value);

    Optional<SecurityMsgValue> securityMsg = securityStore
        .get(SecurityIsinMsgKey.newBuilder().setIsin(value.getIsin()).build());

    if (securityMsg.isPresent()) {
      TradeMsgKey newKey = TradeMsgKey.newBuilder().setTradeId(key.getTradeId()).build();
      TradeMsgValue newValue = TradeMsgValue.newBuilder()
          .setTradeId(value.getTradeId())
          .setSecurityId(securityMsg.get().getSecurityId())
          .setCounterparty(value.getCounterparty())
          .setPortfolio(value.getPortfolio())
          .setQty(value.getQty())
          .build();

      return KeyValue.pair(newKey, newValue);
    } else {
      log.warn("Could not enrich trade {} | {}", key, value);
      return null;
    }
  }

  @PunctuateMethod("${punctuate.interval}")
  public void doThing() {
    log.info("Running punctuate");
  }

  @PunctuateMethod("PT30s")
  public void logAnother() {
    log.info("Running shorter punctuate");
  }

  @Override
  public Collection<TaskStoreRef<?, ?>> taskStores() {
    return null;
  }
}
