package org.galatea.kafka.starter.messaging.streams;

import java.time.Instant;
import java.util.Collection;
import java.util.LinkedList;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
@RequiredArgsConstructor
class FromTemplateProcessor<K, V, T> implements Processor<K, V> {

  private final ProcessorTemplate<K, V, T> processorTemplate;
  private TaskContext context;
  private final Collection<Cancellable> scheduledPunctuates = new LinkedList<>();
  private StoreProvider storeProvider;
  private T state;

  @Override
  public void init(ProcessorContext pContext) {
    this.context = new TaskContext(pContext);
    StreamMdcLoggingUtil.setStreamTask(context.taskId().toString());

    state = processorTemplate.getStateInitializer().init();

    // TODO: create punctuate that will clean stores using retentionPolicy

    Collection<ProcessorPunctuate<T>> punctuates = processorTemplate
        .getPunctuates();
    storeProvider = new StoreProvider(pContext);

    punctuates.forEach(p -> log.info("Scheduling punctuate {}", p));
    for (ProcessorPunctuate<T> punctuate : punctuates) {
      Cancellable scheduled = pContext.schedule(punctuate.getInterval(), punctuate.getType(),
          timestamp -> {
            StreamMdcLoggingUtil.setStreamTask(context.taskId().toString());
            punctuate.getMethod()
                .punctuate(Instant.ofEpochMilli(timestamp), context, state);
            StreamMdcLoggingUtil.removeStreamTask();
          });
      scheduledPunctuates.add(scheduled);
    }
    processorTemplate.getInitMethod().init(storeProvider, state, context);
    StreamMdcLoggingUtil.removeStreamTask();
  }

  @Override
  public void process(K key, V value) {
    StreamMdcLoggingUtil.setStreamTask(context.taskId().toString());
    processorTemplate.getTransformMethod()
        .process(key, value, storeProvider, context, state);
    StreamMdcLoggingUtil.removeStreamTask();
  }

  @Override
  public void close() {
    StreamMdcLoggingUtil.setStreamTask(context.taskId().toString());
    scheduledPunctuates.forEach(Cancellable::cancel);
    scheduledPunctuates.clear();
    processorTemplate.getCloseMethod().close(storeProvider, state, context);
    StreamMdcLoggingUtil.removeStreamTask();
  }

}
