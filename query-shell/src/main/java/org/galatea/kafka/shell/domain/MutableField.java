package org.galatea.kafka.shell.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class MutableField<T> {

  private T inner;

  public T get() {
    return inner;
  }

  public void set(T value) {
    inner = value;
  }

  @SuppressWarnings("unchecked")
  public void setInner(Object inner) {
    this.inner = (T) inner;
  }

  public String toString() {
    return inner.toString();
  }

}
