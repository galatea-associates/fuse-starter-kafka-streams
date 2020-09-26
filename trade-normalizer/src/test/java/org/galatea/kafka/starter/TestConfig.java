package org.galatea.kafka.starter;

import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.galatea.kafka.starter.testing.bean.DefaultBeanRules;
import org.galatea.kafka.starter.testing.bean.SubstitutionUtil;
import org.mockito.stubbing.Answer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class TestConfig {

  @Bean
  SubstitutionUtil beanProcessor() {
    return new SubstitutionUtil()
        .withRule(DefaultBeanRules.topicSerdes())
        .withRule(DefaultBeanRules.adminClient());
  }

  @Bean
  @DependsOn("beanProcessor")
  Object mockAdminClientResponse(AdminClient adminClient) {

    when(adminClient.describeTopics(anyCollection()))
        .thenAnswer((Answer<DescribeTopicsResult>) invocationOnMock -> {

          Collection<String> topics = invocationOnMock.getArgument(0);

          DescribeTopicsResult describeMock = mock(DescribeTopicsResult.class);
          Map<String, TopicDescription> result = new HashMap<>();
          topics.forEach(topic -> result
              .put(topic, new TopicDescription(topic, false, mockPartitions())));
          when(describeMock.all()).thenReturn(KafkaFuture.completedFuture(result));
          return describeMock;
        });

    return new Object();
  }

  private static List<TopicPartitionInfo> mockPartitions() {
    List<TopicPartitionInfo> partitions = new LinkedList<>();

    Node node = new Node(0, "localhost", 65535);
    for (int i = 0; i < 1; i++) {
      partitions.add(new TopicPartitionInfo(i, node, Collections.singletonList(node),
          Collections.singletonList(node)));
    }
    return partitions;
  }
}
