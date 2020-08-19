package org.galatea.kafka.starter.messaging.streams;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.galatea.kafka.starter.messaging.streams.annotate.PunctuateMethod;
import org.galatea.kafka.starter.messaging.streams.exception.IllegalTopologyException;
import org.galatea.kafka.starter.messaging.streams.util.PunctuateArgSupplier;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class PunctuateAnnotationUtil {

  static <K, V, T> Collection<ProcessorPunctuate<K, V, T>> getAnnotatedPunctuates(
      StatefulTransformerRef<?, ?, K, V, T> statefulTransformerRef) {

    Collection<ProcessorPunctuate<K, V, T>> punctuates = new LinkedList<>();
    Class<? extends StatefulTransformerRef> refClass = statefulTransformerRef.getClass();
    for (Method method : refClass.getMethods()) {
      if (method.isAnnotationPresent(PunctuateMethod.class)) {
        if (!Modifier.isPublic(method.getModifiers())
            || !method.getReturnType().getTypeName().equalsIgnoreCase("void")) {
          throw new IllegalTopologyException(String.format("Punctuate method %s#%s must be public "
              + "with void return type", refClass.getName(), method.getName()));
        }
        method.setAccessible(true);
        String annotationInterval = method.getAnnotation(PunctuateMethod.class).value();

        Duration parsedDuration;
        try {
          annotationInterval = tryResolve(statefulTransformerRef.getBeanFactory(), annotationInterval);
          parsedDuration = Duration.parse(annotationInterval);
        } catch (RuntimeException e) {
          throw new IllegalTopologyException(String.format("Illegal Duration interval %s in %s "
                  + "annotation on %s#%s", annotationInterval, PunctuateMethod.class.getSimpleName(),
              refClass.getName(), method.getName()));
        }
        List<PunctuateArgSupplier<K, V, T, Object>> argSuppliers = getPunctuateArgSuppliers(method);

        punctuates.add(punctuateFrom(statefulTransformerRef, method, parsedDuration, argSuppliers));
      }
    }
    return punctuates;
  }

  private static String tryResolve(ConfigurableBeanFactory beanFactory, String value) {
    return beanFactory != null ? beanFactory.resolveEmbeddedValue(value) : value;
  }

  private static <K, V, T> List<PunctuateArgSupplier<K, V, T, Object>> getPunctuateArgSuppliers(
      Method method) {
    List<PunctuateArgSupplier<K, V, T, Object>> argSuppliers = new LinkedList<>();
    for (Class<?> parameterType : method.getParameterTypes()) {
      if (parameterType.equals(Long.class)) {
        argSuppliers.add((timestamp, taskContext) -> timestamp.toEpochMilli());
      } else if (parameterType.equals(Instant.class)) {
        argSuppliers.add((timestamp, taskContext) -> timestamp);
      } else if (TaskContext.class.isAssignableFrom(parameterType)) {
        argSuppliers.add((timestamp, taskContext) -> taskContext);
      } else {
        argSuppliers.add(PunctuateArgSupplier.nullSupplier());
      }
    }
    return argSuppliers;
  }

  private static <K, V, T> ProcessorPunctuate<K, V, T> punctuateFrom(
      StatefulTransformerRef<?, ?, K, V, T> statefulTransformerRef, Method method, Duration interval,
      List<PunctuateArgSupplier<K, V, T, Object>> argSuppliers) {
    return ProcessorPunctuate.<K, V, T>builder()
        .interval(interval)
        .method((timestamp, taskContext) -> {
          Object[] args = argSuppliers.stream().map(as -> as.arg(timestamp, taskContext)).toArray();
          try {
            method.invoke(statefulTransformerRef, args);
          } catch (IllegalAccessException | InvocationTargetException e) {
            log.error("Was not able to invoke punctuate method {}#{}",
                statefulTransformerRef.getClass().getName(), method.getName(), e);
          }
        })
        .build();
  }
}
