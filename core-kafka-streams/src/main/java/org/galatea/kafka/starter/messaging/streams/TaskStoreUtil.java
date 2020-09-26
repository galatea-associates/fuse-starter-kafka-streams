package org.galatea.kafka.starter.messaging.streams;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.galatea.kafka.starter.messaging.streams.annotate.TaskStoreField;
import org.galatea.kafka.starter.messaging.streams.exception.IllegalTopologyException;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class TaskStoreUtil {

  static Set<TaskStoreRef<?, ?>> getTaskStores(TaskStoreSupplier taskStoreSupplier) {
    Set<TaskStoreRef<?, ?>> refs = new HashSet<>(
        Optional.ofNullable(taskStoreSupplier.taskStores()).orElse(new HashSet<>()));
    for (Field field : taskStoreSupplier.getClass().getDeclaredFields()) {
      if (!TaskStoreRef.class.isAssignableFrom(field.getType())) {
        continue;
      }
      TaskStoreField annotation = field.getAnnotation(TaskStoreField.class);
      if (annotation != null) {
        field.setAccessible(true);
        try {
          log.info("Adding task store {}", field.get(taskStoreSupplier));
          refs.add((TaskStoreRef) field.get(taskStoreSupplier));
        } catch (IllegalAccessException e) {
          throw new IllegalTopologyException(e);
        }
      }
    }
    return refs;
  }
}
