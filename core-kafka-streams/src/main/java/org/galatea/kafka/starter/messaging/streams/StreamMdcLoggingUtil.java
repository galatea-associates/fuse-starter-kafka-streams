package org.galatea.kafka.starter.messaging.streams;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.slf4j.MDC;

/**
 * Utility to set the
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StreamMdcLoggingUtil {
  private static final String MDC_STREAM_THREAD_REF = "StreamTask";

  public static void setStreamTask(String taskId) {
    MDC.put(MDC_STREAM_THREAD_REF, "-" + taskId);
  }

  public static void removeStreamTask() {
    MDC.remove(MDC_STREAM_THREAD_REF);
  }
}
