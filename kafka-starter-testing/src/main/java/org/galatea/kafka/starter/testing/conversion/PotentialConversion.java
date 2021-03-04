package org.galatea.kafka.starter.testing.conversion;

import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
@RequiredArgsConstructor
public class PotentialConversion<T> {

  Pattern pattern;
  StringToObjectConversion<T> stringToObjectConversion;
}
