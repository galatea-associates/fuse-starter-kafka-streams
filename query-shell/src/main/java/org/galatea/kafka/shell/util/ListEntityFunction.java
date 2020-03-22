package org.galatea.kafka.shell.util;

import org.apache.kafka.clients.admin.AdminClient;

public interface ListEntityFunction {

  String apply(AdminClient adminClient) throws Exception;
}
