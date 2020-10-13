package org.galatea.kafka.starter.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.galatea.kafka.starter.messaging.security.SecurityIsinMsgKey;
import org.galatea.kafka.starter.messaging.security.SecurityMsgValue;
import org.galatea.kafka.starter.messaging.trade.TradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.TradeMsgValue;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgKey;
import org.galatea.kafka.starter.messaging.trade.input.InputTradeMsgValue;

public class NormalizerTransformer implements
    Transformer<InputTradeMsgKey, InputTradeMsgValue, KeyValue<TradeMsgKey, TradeMsgValue>> {

  private final String securityTableName;
  private ReadOnlyKeyValueStore<SecurityIsinMsgKey, ValueAndTimestamp<SecurityMsgValue>> securityStore;

  public NormalizerTransformer(String securityTableName) {
    this.securityTableName = securityTableName;
  }

  @Override
  public void init(ProcessorContext context) {
    securityStore = (ReadOnlyKeyValueStore<SecurityIsinMsgKey, ValueAndTimestamp<SecurityMsgValue>>) context
        .getStateStore(securityTableName);
  }

  @Override
  public KeyValue<TradeMsgKey, TradeMsgValue> transform(InputTradeMsgKey key,
      InputTradeMsgValue value) {

    SecurityIsinMsgKey securityIsinKey = SecurityIsinMsgKey.newBuilder().setIsin(key.getIsin())
        .build();

    // get the security information from the store
    // by default, KafkaStreams uses value type ValueAndTimestamp that wraps around the actual value we need
    ValueAndTimestamp<SecurityMsgValue> securityMsgValue = securityStore.get(securityIsinKey);

    if (securityMsgValue == null) {
      // if the security isn't in the store, can't normalize
      return null;
    }

    TradeMsgKey newKey = TradeMsgKey.newBuilder()
        .setTradeId(key.getTradeId())
        .build();

    // inherit all values from input, except securityID
    TradeMsgValue newValue = TradeMsgValue.newBuilder()
        .setTradeId(value.getTradeId())

        // ValueAndTimestamp#value() to get the underlying security value
        .setSecurityId(securityMsgValue.value().getSecurityId())
        .setCounterparty(value.getCounterparty())
        .setPortfolio(value.getPortfolio())
        .setQty(value.getQty())
        .build();

    return KeyValue.pair(newKey, newValue);
  }

  @Override
  public void close() {
    // do nothing
  }
}
