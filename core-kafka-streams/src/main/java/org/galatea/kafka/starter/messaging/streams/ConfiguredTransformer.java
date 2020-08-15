package org.galatea.kafka.starter.messaging.streams;

import java.time.Instant;
import java.util.Collection;
import java.util.LinkedList;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.galatea.kafka.starter.messaging.streams.util.ProcessorForwarder;

@Slf4j
@RequiredArgsConstructor
class ConfiguredTransformer<K, V, K1, V1, T> implements Transformer<K, V, KeyValue<K1, V1>> {

  private final TransformerRef<K, V, K1, V1, T> transformerRef;
  private ProcessorTaskContext<K1, V1, T> context;
  private final Collection<Cancellable> scheduledPunctuates = new LinkedList<>();

  @Override
  public void init(ProcessorContext pContext) {
    T state = transformerRef.initState();
    ProcessorForwarder<K1, V1> forwarder = pContext::forward;
    this.context = new ProcessorTaskContext<>(pContext, state, forwarder);

    // TODO: create punctuate that will clean stores using retentionPolicy

    Collection<ProcessorPunctuate<K1, V1, T>> punctuates = new LinkedList<>(
        PunctuateAnnotationUtil.getAnnotatedPunctuates(transformerRef));

    punctuates.forEach(p -> log.info("Scheduling punctuate {}", p));
    for (ProcessorPunctuate<K1, V1, T> punctuate : punctuates) {
      Cancellable scheduled = pContext.schedule(punctuate.getInterval(), punctuate.getType(),
          timestamp -> punctuate.getMethod().punctuate(Instant.ofEpochMilli(timestamp), context));
      scheduledPunctuates.add(scheduled);
    }

  }

  @Override
  public KeyValue<K1, V1> transform(K key, V value) {
    return transformerRef.transform(key, value, context);
  }

  @Override
  public void close() {
    scheduledPunctuates.forEach(Cancellable::cancel);
    scheduledPunctuates.clear();
    transformerRef.close(context);
  }

}
