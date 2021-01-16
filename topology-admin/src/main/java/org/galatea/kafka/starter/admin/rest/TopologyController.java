package org.galatea.kafka.starter.admin.rest;

import java.io.IOException;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import net.sourceforge.plantuml.SourceStringReader;
import org.galatea.kafka.starter.admin.domain.ComponentTopology;
import org.galatea.kafka.starter.admin.domain.RegisterTopologyRequest;
import org.galatea.kafka.starter.admin.service.TopologyParser;
import org.galatea.kafka.starter.admin.service.TopologyRegistrar;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class TopologyController {

  private final TopologyRegistrar service;
  private final TopologyParser parser;

  @PostMapping
  public ResponseEntity<List<RegisterTopologyRequest>> registerTopology(
      @RequestBody List<RegisterTopologyRequest> requests) {
    for (RegisterTopologyRequest request : requests) {
      String topologyUml = parser.parse(request);

      service.registerTopology(
          new ComponentTopology(request.getComponent(), request.getStateNodes(), topologyUml));

    }
    return ResponseEntity.ok(requests);
  }

  @GetMapping(value = "/topology")
  public void getTopology(HttpServletResponse response) throws IOException {
    response.setContentType(MediaType.IMAGE_PNG_VALUE);

    String source = service.getFullTopology();

    SourceStringReader reader = new SourceStringReader(source);
    reader.generateImage(response.getOutputStream());
  }

}
