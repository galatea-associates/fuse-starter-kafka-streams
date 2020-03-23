package org.galatea.kafka.shell.domain;

import java.time.Duration;
import java.time.Instant;

public class ConsumerHistoricalStatistic {

  private static final Duration sampleInterval = Duration.ofSeconds(1);
  private Instant oldestReportTime = Instant.EPOCH;
  private Instant latestReportTime = Instant.EPOCH;
  private long oldestReport = 0;
  private long latestReport = 0;

  public void reportConsumedMessageCount(long consumedMessages) {
    if (latestReportTime.plus(sampleInterval).isAfter(Instant.now())) {
      return;   // only allow updates after sample interval has elapsed
    }
    oldestReportTime = latestReportTime;
    latestReportTime = Instant.now();
    oldestReport = latestReport;
    latestReport = consumedMessages;
  }

  public double messagesPerSecond() {
    long messages = latestReport - oldestReport;
    long msTime = latestReportTime.toEpochMilli() - oldestReportTime.toEpochMilli();
    double seconds = (double) msTime / 1000;
    return messages / seconds;
  }
}
