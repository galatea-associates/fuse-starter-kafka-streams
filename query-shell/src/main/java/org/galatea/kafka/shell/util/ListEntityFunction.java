package org.galatea.kafka.shell.util;

import java.util.List;
import org.apache.kafka.clients.admin.AdminClient;

public interface ListEntityFunction {

  List<String> apply(AdminClient adminClient) throws Exception;
}
