package org.galatea.kafka.starter.messaging;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({BaseStreamingService.class})
public class KafkaStreamsAutoconfig {

}
