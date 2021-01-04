package org.galatea.kafka.starter.messaging.streams;

import java.util.Collection;
import java.util.Collections;

interface TaskStoreSupplier {

  default Collection<TaskStoreRef<?, ?>> taskStores() {
    return Collections.emptyList();
  }
}
