package org.galatea.kafka.shell.util;

import java.util.List;
import org.springframework.core.MethodParameter;
import org.springframework.shell.CompletionContext;
import org.springframework.shell.CompletionProposal;
import org.springframework.shell.standard.ValueProviderSupport;

public class Extractor extends ValueProviderSupport {

  @Override
  public List<CompletionProposal> complete(MethodParameter parameter,
      CompletionContext completionContext, String[] hints) {
    return null;
  }
}
