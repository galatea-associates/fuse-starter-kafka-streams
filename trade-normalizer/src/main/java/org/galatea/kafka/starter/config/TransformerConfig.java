package org.galatea.kafka.starter.config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.security.SecurityIsinMsgKey;
import org.galatea.kafka.starter.messaging.security.SecurityMsgValue;
import org.galatea.kafka.starter.messaging.streams.GlobalStoreRef;
import org.galatea.kafka.starter.messaging.streams.TaskStoreRef;
import org.galatea.kafka.starter.messaging.streams.TransformerTemplate;
import org.galatea.kafka.starter.messaging.trade.TradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.TradeMsgValue;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgValue;
import org.galatea.kafka.starter.service.TradeNormalizationService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class TransformerConfig {

  @Bean
  GlobalStoreRef<SecurityIsinMsgKey, SecurityMsgValue> securityStoreRef(
      Topic<SecurityIsinMsgKey, SecurityMsgValue> securityTopic) {
    return GlobalStoreRef.<SecurityIsinMsgKey, SecurityMsgValue>builder()
        .onTopic(securityTopic)
        .build();
  }

  @Bean
  TaskStoreRef<InputTradeMsgKey, InputTradeMsgValue> tradeStoreRef(
      Topic<InputTradeMsgKey, InputTradeMsgValue> tradeTopic) {
    return TaskStoreRef.<InputTradeMsgKey, InputTradeMsgValue>builder()
        .name("trade")
        .keySerde(tradeTopic.getKeySerde())
        .valueSerde(tradeTopic.getValueSerde())
        .build();
  }

  @Bean
  public TransformerTemplate<InputTradeMsgKey, InputTradeMsgValue, TradeMsgKey, TradeMsgValue, TradeTransformerState> transformerTemplate(
      GlobalStoreRef<SecurityIsinMsgKey, SecurityMsgValue> securityStoreRef,
      TaskStoreRef<InputTradeMsgKey, InputTradeMsgValue> tradeStoreRef) {

    return TransformerTemplate.<InputTradeMsgKey, InputTradeMsgValue, TradeMsgKey, TradeMsgValue, TradeTransformerState>builder()
        .taskStore(tradeStoreRef)

        .stateInitializer(TradeTransformerState::new) // create new empty state object

        // create a new instance of TradeNormalizationService for the local state
        .initMethod((sp, state, context) -> state.setService(new TradeNormalizationService(
            sp.store(securityStoreRef), sp.store(tradeStoreRef)
        )))

        .transformMethod((key, value, sp, context, forwarder, state) -> {

          // use the normalization service
          state.getService().normalize(key, value)
              .forEach(pair -> forwarder.forward(pair.key, pair.value));

          // since records are forwarded using the forwarder, return null to indicate no records to forward here
          return null;
        })
        .build();
  }

  @Data
  private static class TradeTransformerState {

    private TradeNormalizationService service;
  }
}
