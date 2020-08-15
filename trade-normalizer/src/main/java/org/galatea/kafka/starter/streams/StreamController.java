package org.galatea.kafka.starter.streams;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.galatea.kafka.starter.messaging.BaseStreamingService;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.security.SecurityIsinMsgKey;
import org.galatea.kafka.starter.messaging.security.SecurityMsgValue;
import org.galatea.kafka.starter.messaging.streams.GStreamBuilder;
import org.galatea.kafka.starter.messaging.streams.GlobalStoreRef;
import org.galatea.kafka.starter.messaging.trade.TradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.TradeMsgValue;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgValue;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class StreamController extends BaseStreamingService {

  private final Topic<InputTradeMsgKey, InputTradeMsgValue> inputTradeTopic;
//  private final Topic<SecurityIsinMsgKey, SecurityMsgValue> securityTopic;
  private final Topic<TradeMsgKey, TradeMsgValue> normalizedTradeTopic;
  private final TradeTransformer tradeTransformer;
  private final GlobalStoreRef<SecurityIsinMsgKey, SecurityMsgValue> securityStoreRef;

  protected Topology buildTopology() {

    StreamsBuilder builder = new StreamsBuilder();

//    GlobalKTable<SecurityIsinMsgKey, SecurityMsgValue> securityTable = builder
//        .globalTable(securityTopic.getName(), consumedWith(securityTopic));


    GStreamBuilder gBuilder = new GStreamBuilder(builder);
    gBuilder.addGlobalStore(securityStoreRef);

    gBuilder.stream(inputTradeTopic)
        .transform(tradeTransformer)
        .to(normalizedTradeTopic);

//    KStream<InputTradeMsgKey, InputTradeMsgValue> tradeStream = builder
//        .stream(inputTradeTopic.getName(), consumedWith(inputTradeTopic))
//        .peek(this::logConsume);
//
//    KStream<InputTradeMsgKey, TradeMsgValue> securityJoinedTradeStream = tradeStream
//        .join(securityTable,
//            (tradeKey, tradeValue) -> SecurityIsinMsgKey.newBuilder().setIsin(tradeValue.getIsin())
//                .build(),
//            (tradeMsg, securityMsg) -> TradeMsgValue.newBuilder()
//                .setTradeId(tradeMsg.getTradeId())
//                .setSecurityId(securityMsg.getSecurityId())
//                .setCounterparty(tradeMsg.getCounterparty())
//                .setPortfolio(tradeMsg.getPortfolio())
//                .setQty(tradeMsg.getQty())
//                .build());
//
//    KStream<TradeMsgKey, TradeMsgValue> outputTradeStream = securityJoinedTradeStream
//        .selectKey((inputTradeKey, tradeValue) -> TradeMsgKey.newBuilder()
//            .setTradeId(inputTradeKey.getTradeId()).build());
//
//    outputTradeStream.peek(this::logProduce)
//        .to(normalizedTradeTopic.getName(), producedWith(normalizedTradeTopic));

    Topology topology = builder.build();
    log.info("\n{}", topology.describe());
    return topology;
  }

  private <K, V> Consumed<K, V> consumedWith(Topic<K, V> topic) {
    return Consumed.with(topic.getKeySerde(), topic.getValueSerde());
  }

  private <K, V> Produced<K, V> producedWith(Topic<K, V> topic) {
    return Produced.with(topic.getKeySerde(), topic.getValueSerde());
  }
}
