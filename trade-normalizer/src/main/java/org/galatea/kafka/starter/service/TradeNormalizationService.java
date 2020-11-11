package org.galatea.kafka.starter.service;

import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.galatea.kafka.starter.messaging.security.SecurityIsinMsgKey;
import org.galatea.kafka.starter.messaging.security.SecurityMsgValue;
import org.galatea.kafka.starter.messaging.streams.GlobalStore;
import org.galatea.kafka.starter.messaging.streams.TaskStore;
import org.galatea.kafka.starter.messaging.trade.TradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.TradeMsgValue;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgValue;

@Slf4j
@RequiredArgsConstructor
public class TradeNormalizationService {

  private final GlobalStore<SecurityIsinMsgKey, SecurityMsgValue> securityStore;
  private final TaskStore<InputTradeMsgKey, InputTradeMsgValue> receivedTradesStore;

  public Stream<KeyValue<TradeMsgKey, TradeMsgValue>> normalize(InputTradeMsgKey key,
      InputTradeMsgValue value) {
    log.info("Processing {} | {}", key, value);
    Builder<KeyValue<TradeMsgKey, TradeMsgValue>> outputStream = Stream.builder();

    Optional<InputTradeMsgValue> existingTrade = receivedTradesStore.get(key);
    existingTrade.ifPresent(trade -> log.info("Existing trade: {}", trade));
    receivedTradesStore.put(key, value);

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

      outputStream.add(KeyValue.pair(newKey, newValue));
    } else {
      log.warn("Could not enrich trade {} | {}", key, value);
    }
    return outputStream.build();
  }
}
