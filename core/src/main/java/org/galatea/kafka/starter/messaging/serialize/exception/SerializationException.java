package org.galatea.kafka.starter.messaging.serialize.exception;

public class SerializationException extends RuntimeException {

  public SerializationException(String msg) {
    super(msg);
  }

  public SerializationException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public SerializationException(Throwable cause) {
    super(cause);
  }

}
