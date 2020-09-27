package org.galatea.kafka.starter.messaging.streams;

import java.time.Instant;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.galatea.kafka.starter.messaging.streams.util.ProcessorForwarder;
import org.slf4j.MDC;

@Slf4j
@RequiredArgsConstructor
class ConfiguredTransformer<K, V, K1, V1, T> implements Transformer<K, V, KeyValue<K1, V1>> {

  private static final String MDC_STREAM_THREAD_REF = "StreamThread";
  private final StatefulTransformerRef<K, V, K1, V1, T> statefulTransformerRef;
  private ProcessorTaskContext<K1, V1, T> context;
  private final Collection<Cancellable> scheduledPunctuates = new LinkedList<>();

  @Override
  public void init(ProcessorContext pContext) {
    MDC.put(MDC_STREAM_THREAD_REF, "-" + pContext.taskId());

    T state = statefulTransformerRef.initState();
    ProcessorForwarder<K1, V1> forwarder = pContext::forward;
    this.context = new ProcessorTaskContext<>(pContext, state, forwarder);

    // TODO: create punctuate that will clean stores using retentionPolicy

    Collection<ProcessorPunctuate<K1, V1, T>> punctuates = new LinkedList<>(
        PunctuateAnnotationUtil.getAnnotatedPunctuates(statefulTransformerRef));
    Set<TaskStoreRef<?, ?>> taskStores = TaskStoreUtil.getTaskStores(statefulTransformerRef);

    punctuates.forEach(p -> log.info("Scheduling punctuate {}", p));
    for (ProcessorPunctuate<K1, V1, T> punctuate : punctuates) {
      Cancellable scheduled = pContext.schedule(punctuate.getInterval(), punctuate.getType(),
          timestamp -> punctuate.getMethod().punctuate(Instant.ofEpochMilli(timestamp), context));
      scheduledPunctuates.add(scheduled);
    }
    statefulTransformerRef.init(context);
    MDC.remove(MDC_STREAM_THREAD_REF);
  }

  @Override
  public KeyValue<K1, V1> transform(K key, V value) {
    MDC.put(MDC_STREAM_THREAD_REF, "-" + context.taskId());
    KeyValue<K1, V1> transform = statefulTransformerRef.transform(key, value, context);
    MDC.remove(MDC_STREAM_THREAD_REF);
    return transform;
  }

  @Override
  public void close() {
    MDC.put(MDC_STREAM_THREAD_REF, "-" + context.taskId());
    scheduledPunctuates.forEach(Cancellable::cancel);
    scheduledPunctuates.clear();
    statefulTransformerRef.close(context);
    MDC.remove(MDC_STREAM_THREAD_REF);
  }

}
