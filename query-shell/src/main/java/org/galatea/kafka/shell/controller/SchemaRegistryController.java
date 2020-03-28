package org.galatea.kafka.shell.controller;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SchemaRegistryController {

  private final SchemaRegistryClient client;

  public List<String> listSubjects() throws IOException, RestClientException {
    Collection<String> subjects = client.getAllSubjects();
    List<String> outputSubjects = new ArrayList<>(subjects.size());
    for (String subject : subjects) {
      outputSubjects.add(subject + ": v" + client.getAllVersions(subject).toString());
    }
    return outputSubjects;
  }

  public Optional<Schema> describeSchema(String subject, Integer version)
      throws IOException, RestClientException {
    SchemaMetadata metadata = client.getSchemaMetadata(subject, version);
    if (metadata != null) {
      return Optional.ofNullable(client.getById(metadata.getId()));
    }
    return Optional.empty();
  }

  public Optional<SchemaMetadata> getLatesSchemaMetadata(String name)
      throws IOException, RestClientException {
    return Optional.ofNullable(client.getLatestSchemaMetadata(name));
  }
}
