package org.galatea.kafka.shell.util;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import lombok.Getter;

public class RegexPredicate implements Predicate<String> {
  private final Predicate<String> innerPredicate;
  @Getter
  private final String[] regex;

  public RegexPredicate(String[] regex) {
    this.regex = Arrays.copyOf(regex, regex.length);
    List<Pattern> patterns = new LinkedList<>();
    for (String patternString : regex) {
      patterns.add(Pattern.compile(patternString));
    }
    innerPredicate = string -> {
      for (Pattern pattern : patterns) {
        if (!pattern.matcher(string).find()) {
          return false;
        }
      }
      return true;
    };
  }

  public boolean test(String string) {
    return innerPredicate.test(string);
  }
}
