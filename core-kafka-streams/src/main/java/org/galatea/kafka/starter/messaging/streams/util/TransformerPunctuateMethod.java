package org.galatea.kafka.starter.messaging.streams.util;

import java.time.Instant;
import org.galatea.kafka.starter.messaging.streams.TaskContext;

public interface TransformerPunctuateMethod<K1, V1, T> {

    void punctuate(Instant timestamp, TaskContext taskContext, ProcessorForwarder<K1, V1> forwarder,
        T state);
}