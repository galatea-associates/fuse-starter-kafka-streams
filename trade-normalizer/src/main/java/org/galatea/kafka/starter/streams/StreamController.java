package org.galatea.kafka.starter.streams;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.Topology;
import org.galatea.kafka.starter.messaging.Topic;
import org.galatea.kafka.starter.messaging.security.SecurityIsinMsgKey;
import org.galatea.kafka.starter.messaging.security.SecurityMsgValue;
import org.galatea.kafka.starter.messaging.streams.GStreamBuilder;
import org.galatea.kafka.starter.messaging.streams.GlobalStoreRef;
import org.galatea.kafka.starter.messaging.streams.TopologyProvider;
import org.galatea.kafka.starter.messaging.streams.TransformerTemplate;
import org.galatea.kafka.starter.messaging.trade.TradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.TradeMsgValue;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgValue;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class StreamController implements TopologyProvider {

  private final Topic<InputTradeMsgKey, InputTradeMsgValue> inputTradeTopic;
  private final Topic<TradeMsgKey, TradeMsgValue> normalizedTradeTopic;

  // Defined in TransformerConfig
  private final TransformerTemplate<InputTradeMsgKey, InputTradeMsgValue, TradeMsgKey, TradeMsgValue, ?> transformerTemplate;
  private final GlobalStoreRef<SecurityIsinMsgKey, SecurityMsgValue> securityStoreRef;

  @Override
  public Topology buildTopology(GStreamBuilder builder) {
    builder.addGlobalStore(securityStoreRef);

    builder.stream(inputTradeTopic)
        .transform(transformerTemplate)
        .to(normalizedTradeTopic);

    return builder.build();
  }
}
