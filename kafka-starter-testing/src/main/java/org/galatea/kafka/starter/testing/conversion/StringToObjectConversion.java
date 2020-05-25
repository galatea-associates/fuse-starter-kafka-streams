package org.galatea.kafka.starter.testing.conversion;

import java.util.function.BiFunction;
import java.util.regex.Matcher;

public interface StringToObjectConversion<T> extends BiFunction<String, Matcher, T> {

}
