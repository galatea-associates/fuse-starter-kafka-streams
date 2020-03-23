package org.galatea.kafka.shell.consumer.request;

import java.util.concurrent.Semaphore;
import lombok.Getter;
import org.apache.kafka.clients.consumer.Consumer;

public abstract class ConsumerRequest<T> {

  @Getter
  private boolean isComplete = false;
  private Semaphore semaphore = new Semaphore(0);
  private T result;
  abstract T fulfillRequest(Consumer<byte[], byte[]> consumer);

  final public void internalFulfillRequest(Consumer<byte[], byte[]> consumer) {
    result = fulfillRequest(consumer);
    isComplete = true;
    semaphore.release();
  }

  public T get() throws InterruptedException {
    semaphore.acquire();
    return result;
  }
}
