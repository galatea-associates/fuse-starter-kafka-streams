package org.galatea.kafka.starter.testing.bean;

import java.util.Collection;
import java.util.LinkedList;
import org.galatea.kafka.starter.testing.bean.domain.ReplacementRule;
import org.galatea.kafka.starter.testing.bean.domain.SpringBeanData;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

public class SubstitutionUtil implements BeanPostProcessor {

  private final Collection<ReplacementRule> rules = new LinkedList<>();

  public SubstitutionUtil withRule(ReplacementRule rule) {
    rules.add(rule);
    return this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object postProcessBeforeInitialization(Object bean, String beanName)
      throws BeansException {
    SpringBeanData<?> beanData = new SpringBeanData<>(beanName, bean);
    for (ReplacementRule rule : rules) {
      if (rule.getPredicate().test(beanData)) {
        return rule.getSupplier().get(beanData);
      }
    }
    return bean;
  }
}
