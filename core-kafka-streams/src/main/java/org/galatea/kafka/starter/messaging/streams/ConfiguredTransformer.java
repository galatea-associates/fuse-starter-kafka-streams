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
import org.slf4j.MDC;

@Slf4j
@RequiredArgsConstructor
class ConfiguredTransformer<K, V, K1, V1, T> implements Transformer<K, V, KeyValue<K1, V1>> {

    private static final String MDC_STREAM_THREAD_REF = "StreamThread";
    private final TransformerTemplate<K, V, K1, V1, T> transformerTemplate;
    private TaskContext context;
    private final Collection<Cancellable> scheduledPunctuates = new LinkedList<>();
    private StoreProvider storeProvider;
    private ProcessorForwarder<K1, V1> forwarder;
    private T state;

    @Override
    public void init(ProcessorContext pContext) {
        MDC.put(MDC_STREAM_THREAD_REF, "-" + pContext.taskId());

        state = transformerTemplate.getStateInitializer().init();
        forwarder = pContext::forward;
        this.context = new TaskContext(pContext);

        // TODO: create punctuate that will clean stores using retentionPolicy

        Collection<TransformerPunctuate<K1, V1, T>> punctuates = transformerTemplate
            .getPunctuates();
        storeProvider = new StoreProvider(pContext);

        punctuates.forEach(p -> log.info("Scheduling punctuate {}", p));
        for (TransformerPunctuate<K1, V1, T> punctuate : punctuates) {
            Cancellable scheduled = pContext.schedule(punctuate.getInterval(), punctuate.getType(),
                timestamp -> punctuate.getMethod()
                    .punctuate(Instant.ofEpochMilli(timestamp), context, forwarder, state));
            scheduledPunctuates.add(scheduled);
        }
        transformerTemplate.getInitMethod().init(storeProvider, state, context);
        MDC.remove(MDC_STREAM_THREAD_REF);
    }

    @Override
    public KeyValue<K1, V1> transform(K key, V value) {
        MDC.put(MDC_STREAM_THREAD_REF, "-" + context.taskId());
        KeyValue<K1, V1> transform = transformerTemplate.getTransformMethod()
            .transform(key, value, storeProvider, context, forwarder, state);
        MDC.remove(MDC_STREAM_THREAD_REF);
        return transform;
    }

    @Override
    public void close() {
        MDC.put(MDC_STREAM_THREAD_REF, "-" + context.taskId());
        scheduledPunctuates.forEach(Cancellable::cancel);
        scheduledPunctuates.clear();
        transformerTemplate.getCloseMethod().close(storeProvider, state, context);
        MDC.remove(MDC_STREAM_THREAD_REF);
    }

}
