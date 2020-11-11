package org.galatea.kafka.starter.messaging.streams.exception;

public class IllegalTopologyException extends RuntimeException {

  public IllegalTopologyException(String msg) {
    super(msg);
  }

  public IllegalTopologyException(Throwable e) {
    super(e);
  }

  public IllegalTopologyException(String msg, Throwable e) {
    super(msg, e);
  }
}
