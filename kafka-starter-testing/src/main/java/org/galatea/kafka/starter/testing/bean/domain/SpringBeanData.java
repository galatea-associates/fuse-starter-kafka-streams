package org.galatea.kafka.starter.testing.bean.domain;

import lombok.Value;

@Value
public class SpringBeanData<T> {

  String beanName;
  T bean;
}
