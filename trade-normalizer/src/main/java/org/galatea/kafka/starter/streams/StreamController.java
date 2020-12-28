package org.galatea.kafka.starter.streams;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.galatea.kafka.starter.messaging.BaseStreamingService;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.security.SecurityIsinMsgKey;
import org.galatea.kafka.starter.messaging.security.SecurityMsgValue;
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
  private final Topic<SecurityIsinMsgKey, SecurityMsgValue> securityTopic;
  private final Topic<TradeMsgKey, TradeMsgValue> normalizedTradeTopic;

  public Topology buildTopology() {

    StreamsBuilder builder = new StreamsBuilder();

    GlobalKTable<SecurityIsinMsgKey, SecurityMsgValue> securityTable = builder
        .globalTable(securityTopic.getName(), consumedWith(securityTopic));

    KStream<InputTradeMsgKey, InputTradeMsgValue> tradeStream = builder
        .stream(inputTradeTopic.getName(), consumedWith(inputTradeTopic))
        .transformValues(this::peekTransformer)
        .map(
            (KeyValueMapper<InputTradeMsgKey, InputTradeMsgValue, KeyValue<InputTradeMsgKey, InputTradeMsgValue>>) (key, value) -> {
              key.setTradeId(key.getTradeId() + "-1");
              return KeyValue.pair(key, value);
            })
        .transformValues(this::peekTransformer)
        .through("repartition-topic", producedWith(inputTradeTopic))
        .transformValues(this::peekTransformer);
//        .peek(this::logConsume);

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
        .to(normalizedTradeTopic.getName(), producedWith(normalizedTradeTopic));

    Topology topology = builder.build();
    log.info("\n{}", topology.describe());
    return topology;
  }

  private ValueTransformerWithKey<InputTradeMsgKey, InputTradeMsgValue, InputTradeMsgValue> peekTransformer() {
    return new ValueTransformerWithKey<InputTradeMsgKey, InputTradeMsgValue, InputTradeMsgValue>() {
      private ProcessorContext context;

      @Override
      public void init(ProcessorContext context) {
        this.context = context;
      }

      @Override
      public InputTradeMsgValue transform(InputTradeMsgKey key,
          InputTradeMsgValue value) {
        log.info("{} Consumed [{}|{}]: {} | {} ", context.taskId(),
            classNameDisplay(key), classNameDisplay(value), key, value);
        return value;
      }

      @Override
      public void close() {

      }
    };
  }

  private <K, V> Consumed<K, V> consumedWith(Topic<K, V> topic) {
    return Consumed.with(topic.getKeySerde(), topic.getValueSerde());
  }

  private <K, V> Produced<K, V> producedWith(Topic<K, V> topic) {
    return Produced.with(topic.getKeySerde(), topic.getValueSerde());
  }

  private String classNameDisplay(Object obj) {
    return obj == null ? "null" : obj.getClass().getSimpleName();
  }
}
