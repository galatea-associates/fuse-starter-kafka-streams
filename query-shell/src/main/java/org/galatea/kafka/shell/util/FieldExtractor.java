package org.galatea.kafka.shell.util;

import java.util.function.Function;
import org.galatea.kafka.shell.domain.MutableField;

public interface FieldExtractor<T> extends Function<T, MutableField<?>> {

}
