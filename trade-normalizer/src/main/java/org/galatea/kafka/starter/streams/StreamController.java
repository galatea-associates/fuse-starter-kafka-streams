package org.galatea.kafka.starter.streams;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.galatea.kafka.starter.messaging.BaseStreamingService;
import org.galatea.kafka.starter.messaging.StreamProperties;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.security.SecurityIsinMsgKey;
import org.galatea.kafka.starter.messaging.security.SecurityMsgValue;
import org.galatea.kafka.starter.messaging.trade.TradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.TradeMsgValue;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgValue;

@Slf4j
@RequiredArgsConstructor
public class StreamController extends BaseStreamingService {

  private final StreamProperties properties;
  private final Topic<InputTradeMsgKey, InputTradeMsgValue> inputTradeTopic;
  private final Topic<SecurityIsinMsgKey, SecurityMsgValue> securityTopic;
  private final Topic<TradeMsgKey, TradeMsgValue> normalizedTradeTopic;

  protected Topology buildTopology() {
    streamProperties = properties.asProperties();

    StreamsBuilder builder = new StreamsBuilder();
    GlobalKTable<SecurityIsinMsgKey, SecurityMsgValue> securityTable = builder
        .globalTable(securityTopic.getName(), securityTopic.consumedWith());

    KStream<InputTradeMsgKey, InputTradeMsgValue> tradeStream = builder
        .stream(inputTradeTopic.getName(), inputTradeTopic.consumedWith())
        .peek(this::logConsume);

    KStream<InputTradeMsgKey, TradeMsgValue> securityJoinedTradeStream = tradeStream
        .join(securityTable,
            (tradeKey, tradeValue) -> SecurityIsinMsgKey.newBuilder().setIsin(tradeValue.getIsin())
                .build(),
            (tradeMsg, securityMsg) -> TradeMsgValue.newBuilder()
                .setTradeId(tradeMsg.getTradeId())
                .setSecurityId(securityMsg.getSecurityId())
                .setCounterparty(tradeMsg.getCounterparty())
                .setPortfolio(tradeMsg.getPortfolio())
                .setQty(tradeMsg.getQty())
                .build());

    KStream<TradeMsgKey, TradeMsgValue> outputTradeStream = securityJoinedTradeStream
        .selectKey((inputTradeKey, tradeValue) -> TradeMsgKey.newBuilder()
            .setTradeId(inputTradeKey.getTradeId()).build());

    outputTradeStream.peek(this::logProduce)
        .to(normalizedTradeTopic.getName(), normalizedTradeTopic.producedWith());

    return builder.build();
  }
}
