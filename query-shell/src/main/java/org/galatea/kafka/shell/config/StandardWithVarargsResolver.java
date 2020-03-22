/**
 * Copied {@link org.springframework.shell.standard.StandardParameterResolver#resolve(org.springframework.core.MethodParameter,
 * java.util.List)} and supporting private methods as basis for {@link
 * org.galatea.kafka.shell.config.StandardWithVarargsResolver#resolve(org.springframework.core.MethodParameter,
 * java.util.List)}. Added option for {@link org.galatea.kafka.shell.config.StandardWithVarargsResolver#VARARGS_ARITY}
 * that can be used when parsing arguments as positional
 */
package org.galatea.kafka.shell.config;

import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.core.MethodParameter;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.shell.CompletionContext;
import org.springframework.shell.ParameterMissingResolutionException;
import org.springframework.shell.UnfinishedParameterResolutionException;
import org.springframework.shell.Utils;
import org.springframework.shell.ValueResult;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import org.springframework.shell.standard.StandardParameterResolver;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.ConcurrentReferenceHashMap;

@Component
public class StandardWithVarargsResolver extends StandardParameterResolver {

  private final Map<CacheKey, Map<Parameter, ParameterRawValue>> parameterCache = new ConcurrentReferenceHashMap<>();
  private final ConversionService conversionService;
  /**
   * Can only be used as the last argument in a shell command, as the argument that uses this will
   * consume all remaining arguments leaving none for any arguments afterwards
   */
  public static final int VARARGS_ARITY = -2;

  public StandardWithVarargsResolver(
      ConversionService conversionService) {
    super(conversionService);
    this.conversionService = conversionService;
  }

  @Override
  public ValueResult resolve(MethodParameter methodParameter, List<String> wordsBuffer) {

    List<String> words = wordsBuffer.stream().filter(w -> !w.isEmpty())
        .collect(Collectors.toList());

    CacheKey cacheKey = new CacheKey(methodParameter.getMethod(), wordsBuffer);
    parameterCache.clear();
    Map<Parameter, ParameterRawValue> resolved = parameterCache.computeIfAbsent(cacheKey, (k) -> {

      Map<Parameter, ParameterRawValue> result = new HashMap<>();
      Map<String, String> namedParameters = new HashMap<>();

      // index of words that haven't yet been used to resolve parameter values
      List<Integer> unusedWords = new ArrayList<>();

      Set<String> possibleKeys = gatherAllPossibleKeys(methodParameter.getMethod());

      // First, resolve all parameters passed by-name
      for (int i = 0; i < words.size(); i++) {
        int from = i;
        String word = words.get(i);
        if (possibleKeys.contains(word)) {
          String key = word;
          Parameter parameter = lookupParameterForKey(methodParameter.getMethod(), key);
          int arity = getArity(parameter);

          if (i + 1 + arity > words.size()) {
            String input = String.join(" ", words.subList(i, words.size()));
            throw new UnfinishedParameterResolutionException(
                describe(Utils.createMethodParameter(parameter)).findFirst().get(), input);
          }
          Assert.isTrue(i + 1 + arity <= words.size(),
              String.format("Not enough input for parameter '%s'", word));
          String raw = String.join(",", words.subList(i + 1, i + 1 + arity));
          Assert.isTrue(!namedParameters.containsKey(key),
              String.format("Parameter for '%s' has already been specified", word));
          namedParameters.put(key, raw);
          if (arity == 0) {
            boolean defaultValue = booleanDefaultValue(parameter);
            // Boolean parameter has been specified. Use the opposite of the default value
            result.put(parameter,
                ParameterRawValue.explicit(String.valueOf(!defaultValue), key, from, from));
          } else {
            i += arity;
            result.put(parameter, ParameterRawValue.explicit(raw, key, from, i));
          }
        } // store for later processing of positional params
        else {
          unusedWords.add(i);
        }
      }

      // Now have a second pass over params and treat them as positional
      int offset = 0;
      Parameter[] parameters = methodParameter.getMethod().getParameters();
      for (int i = 0, parametersLength = parameters.length; i < parametersLength; i++) {
        Parameter parameter = parameters[i];
        // Compute the intersection between possible keys for the param and what we've already
        // seen for named params
        Collection<String> copy = getKeysForParameter(methodParameter.getMethod(), i)
            .collect(Collectors.toSet());
        copy.retainAll(namedParameters.keySet());
        if (copy.isEmpty()) { // Was not set via a key (including aliases), must be positional
          int arity = getArity(parameter);
          if (arity == VARARGS_ARITY && i == parametersLength - 1) {
            // varargs arity can only be used as the last argument as it will consume all remaining
            List<String> remainingWords = unusedWords.subList(offset, unusedWords.size()).stream()
                .map(words::get).collect(Collectors.toList());
            String raw = String.join(",", remainingWords);
            int from = unusedWords.get(offset);
            int to = from + remainingWords.size();
            result.put(parameter, ParameterRawValue.explicit(raw, null, from, to));
            offset += remainingWords.size();
          } else if (arity > 0 && (offset + arity) <= unusedWords.size()) {
            String raw = unusedWords.subList(offset, offset + arity).stream().map(words::get)
                .collect(Collectors.joining(","));
            int from = unusedWords.get(offset);
            int to = from + arity - 1;
            result.put(parameter, ParameterRawValue.explicit(raw, null, from, to));
            offset += arity;
          } // No more input. Try defaultValues
          else {
            Optional<String> defaultValue = defaultValueFor(parameter);
            defaultValue.ifPresent(
                value -> result
                    .put(parameter, ParameterRawValue.implicit(value, null, null, null)));
          }
        } else if (copy.size() > 1) {
          throw new IllegalArgumentException(
              "Named parameter has been specified multiple times via " + quote(copy));
        }
      }

      Assert.isTrue(offset == unusedWords.size(),
          "Too many arguments: the following could not be mapped to parameters: "
              + unusedWords.subList(offset, unusedWords.size()).stream()
              .map(words::get).collect(Collectors.joining(" ", "'", "'")));
      return result;
    });

    Parameter param = methodParameter.getMethod().getParameters()[methodParameter
        .getParameterIndex()];
    if (!resolved.containsKey(param)) {
      throw new ParameterMissingResolutionException(describe(methodParameter).findFirst().get());
    }
    StandardWithVarargsResolver.ParameterRawValue parameterRawValue = resolved.get(param);
    Object value = convertRawValue(parameterRawValue, methodParameter);
    BitSet wordsUsed = getWordsUsed(parameterRawValue);
    BitSet wordsUsedForValue = getWordsUsedForValue(parameterRawValue);
    return new ValueResult(methodParameter, value, wordsUsed, wordsUsedForValue);
  }

  private int getArity(Parameter parameter) {
    ShellOption option = parameter.getAnnotation(ShellOption.class);
    int inferred =
        (parameter.getType() == boolean.class || parameter.getType() == Boolean.class) ? 0 : 1;
    return option != null && option.arity() != ShellOption.ARITY_USE_HEURISTICS ? option.arity()
        : inferred;
  }

  private String prefixForMethod(Executable method) {
    return method.getAnnotation(ShellMethod.class).prefix();
  }

  private Stream<String> getKeysForParameter(Parameter p) {
    Executable method = p.getDeclaringExecutable();
    String prefix = prefixForMethod(method);
    ShellOption option = p.getAnnotation(ShellOption.class);
    if (option != null && option.value().length > 0) {
      return Arrays.stream(option.value());
    } else {
      return Stream
          .of(prefix + Utils.unCamelify(Utils.createMethodParameter(p).getParameterName()));
    }
  }

  private Set<String> gatherAllPossibleKeys(Method method) {
    return Arrays.stream(method.getParameters())
        .flatMap(this::getKeysForParameter)
        .collect(Collectors.toSet());
  }


  /**
   * Return the key(s) for the i-th parameter of the command method, resolved either from the {@link
   * ShellOption} annotation, or from the actual parameter name.
   */
  private Stream<String> getKeysForParameter(Method method, int index) {
    Parameter p = method.getParameters()[index];
    return getKeysForParameter(p);
  }

  /**
   * Return the method parameter that should be bound to the given key.
   */
  private Parameter lookupParameterForKey(Method method, String key) {
    Parameter[] parameters = method.getParameters();
    for (int i = 0, parametersLength = parameters.length; i < parametersLength; i++) {
      Parameter p = parameters[i];
      if (getKeysForParameter(method, i).anyMatch(k -> k.equals(key))) {
        return p;
      }
    }
    throw new IllegalArgumentException(
        String.format("Could not look up parameter for '%s' in %s", key, method));
  }


  private boolean booleanDefaultValue(Parameter parameter) {
    ShellOption option = parameter.getAnnotation(ShellOption.class);
    if (option != null && !ShellOption.NONE.equals(option.defaultValue())) {
      return Boolean.parseBoolean(option.defaultValue());
    }
    return false;
  }


  private Optional<String> defaultValueFor(Parameter parameter) {
    ShellOption option = parameter.getAnnotation(ShellOption.class);
    if (option != null && !ShellOption.NONE.equals(option.defaultValue())) {
      return Optional.of(option.defaultValue());
    } else if (getArity(parameter) == 0) {
      return Optional.of("false");
    }
    return Optional.empty();
  }

  private BitSet getWordsUsedForValue(ParameterRawValue parameterRawValue) {
    if (parameterRawValue.from != null) {
      BitSet wordsUsedForValue = new BitSet();
      wordsUsedForValue.set(parameterRawValue.from, parameterRawValue.to + 1);
      if (parameterRawValue.key != null) {
        wordsUsedForValue.clear(parameterRawValue.from);
      }
      return wordsUsedForValue;
    }
    return null;
  }

  private BitSet getWordsUsed(ParameterRawValue parameterRawValue) {
    if (parameterRawValue.from != null) {
      BitSet wordsUsed = new BitSet();
      wordsUsed.set(parameterRawValue.from, parameterRawValue.to + 1);
      return wordsUsed;
    }
    return null;
  }

  private Object convertRawValue(ParameterRawValue parameterRawValue,
      MethodParameter methodParameter) {
    String s = parameterRawValue.value;
    if (ShellOption.NULL.equals(s)) {
      return null;
    } else {
      return conversionService.convert(s, TypeDescriptor.valueOf(String.class),
          new TypeDescriptor(methodParameter));
    }
  }

  /**
   * Surrounds the parameter keys with quotes.
   */
  private String quote(Collection<String> keys) {
    return keys.stream().collect(Collectors.joining(", ", "'", "'"));
  }


  private static class ParameterRawValue {

    private CompletionContext context;

    private Integer from;

    private Integer to;

    private Integer keyIndex;

    /**
     * The raw String value that got bound to a parameter.
     */
    private final String value;

    /**
     * If false, the value resolved is the result of applying defaults.
     */
    private final boolean explicit;

    /**
     * The key that was used to set the parameter, or null if resolution happened by position.
     */
    private final String key;

    private ParameterRawValue(String value, boolean explicit, String key, Integer from,
        Integer to) {
      this.value = value;
      this.explicit = explicit;
      this.key = key;
      this.from = from;
      this.to = to;
    }

    public static ParameterRawValue explicit(String value, String key, Integer from, Integer to) {
      return new ParameterRawValue(value, true, key, from, to);
    }

    public static ParameterRawValue implicit(String value, String key, Integer from, Integer to) {
      return new ParameterRawValue(value, false, key, from, to);
    }

    public boolean positional() {
      return key == null;
    }

    @Override
    public String toString() {
      return "ParameterRawValue{" +
          "value='" + value + '\'' +
          ", explicit=" + explicit +
          ", key='" + key + '\'' +
          ", from=" + from +
          ", to=" + to +
          '}';
    }
  }

  private static class CacheKey {

    private final Method method;

    private final List<String> words;

    private CacheKey(Method method, List<String> words) {
      this.method = method;
      this.words = words;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CacheKey cacheKey = (CacheKey) o;
      return Objects.equals(method, cacheKey.method) &&
          Objects.equals(words, cacheKey.words);
    }

    @Override
    public int hashCode() {
      return Objects.hash(method, words);
    }

    @Override
    public String toString() {
      return method.getName() + " " + words;
    }
  }
}
