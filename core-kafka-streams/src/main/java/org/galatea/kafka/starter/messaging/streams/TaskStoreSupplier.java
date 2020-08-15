package org.galatea.kafka.starter.messaging.streams;

import java.util.Collection;

interface TaskStoreSupplier {

  Collection<TaskStoreRef<?, ?>> taskStores();
}
