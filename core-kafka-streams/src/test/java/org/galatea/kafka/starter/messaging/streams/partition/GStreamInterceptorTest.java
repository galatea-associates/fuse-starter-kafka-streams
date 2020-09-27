package org.galatea.kafka.starter.messaging.streams.partition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.galatea.kafka.starter.messaging.streams.domain.ConfiguredHeaders;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

public class GStreamInterceptorTest {

  private AdminClient adminClient;
  private GStreamInterceptor<Object, Object> interceptor;

  @Before
  public void setup() {
    adminClient = mock(AdminClient.class);

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

    GStreamInterceptor.setKafkaAdminClient(null);
    GStreamInterceptor.getTopicPartitions().clear();
    interceptor = new GStreamInterceptor<>();
  }

  @Test
  public void adminClientNotSet_recordWithoutPartitionKey() {
    ProducerRecord<Object, Object> record = interceptor
        .onSend(new ProducerRecord<>("topic", "test", "one"));

    assertNull(record.partition());
  }

  @Test
  public void adminClientSet_recordWithoutPartitionKey() {
    GStreamInterceptor.setKafkaAdminClient(adminClient);
    ProducerRecord<Object, Object> record = interceptor
        .onSend(new ProducerRecord<>("topic", "test", "one"));

    assertNull(record.partition());
  }

  @Test(expected = RuntimeException.class)
  public void adminClientNotSet_recordWithPartitionKey() {
    Collection<Header> headers = new LinkedList<>();
    headers.add(new RecordHeader(ConfiguredHeaders.NEW_PARTITION_KEY.getKey(), "key".getBytes()));

    // when
    interceptor.onSend(new ProducerRecord<>("topic", null, "test", "one", headers));
  }

  @Test
  public void adminClientSet_recordWithPartitionKey() {
    GStreamInterceptor.setKafkaAdminClient(adminClient);
    Collection<Header> headers = new LinkedList<>();
    headers.add(new RecordHeader(ConfiguredHeaders.NEW_PARTITION_KEY.getKey(), "key".getBytes()));

    // when
    ProducerRecord<Object, Object> record = interceptor
        .onSend(new ProducerRecord<>("topic", null, "test", "one", headers));

    //then
    assertNotNull(record.partition());
  }

  @Test
  public void recordWithPartitionKey_PreassignedPartition() {
    GStreamInterceptor.setKafkaAdminClient(adminClient);
    Collection<Header> headers = new LinkedList<>();
    headers.add(new RecordHeader(ConfiguredHeaders.NEW_PARTITION_KEY.getKey(), "key".getBytes()));

    // when
    ProducerRecord<Object, Object> record = interceptor
        .onSend(new ProducerRecord<>("topic", 7, "test", "one", headers));

    //then
    assertEquals(7, (int) record.partition());
  }

  @Test
  public void recordWithoutPartitionKey_PreassignedPartition() {
    GStreamInterceptor.setKafkaAdminClient(adminClient);
    // when
    ProducerRecord<Object, Object> record = interceptor
        .onSend(new ProducerRecord<>("topic", 7, "test", "one"));

    //then
    assertEquals(7, (int) record.partition());
  }

  @Test
  public void recordWithoutPartitionKey_WithOtherHeaders() {
    GStreamInterceptor.setKafkaAdminClient(adminClient);
    Collection<Header> headers = new LinkedList<>();
    headers.add(new RecordHeader("randomKey", "key".getBytes()));

    // when
    ProducerRecord<Object, Object> record = interceptor
        .onSend(new ProducerRecord<>("topic", null, "test", "one", headers));

    //then
    assertNull(record.partition());
  }

  @Test
  public void recordWithPartitionKey_WithOtherHeaders() {
    GStreamInterceptor.setKafkaAdminClient(adminClient);
    Collection<Header> headers = new LinkedList<>();
    headers.add(new RecordHeader("randomKey", "key".getBytes()));
    headers.add(new RecordHeader(ConfiguredHeaders.NEW_PARTITION_KEY.getKey(), "key".getBytes()));

    // when
    ProducerRecord<Object, Object> record = interceptor
        .onSend(new ProducerRecord<>("topic", null, "test", "one", headers));

    //then
    assertNotNull(record.partition());
  }

  @Test
  public void recordWithMultiplePartitionKey_useFirstAdded() {
    GStreamInterceptor.setKafkaAdminClient(adminClient);
    Collection<Header> headers = new LinkedList<>();
    headers.add(new RecordHeader(ConfiguredHeaders.NEW_PARTITION_KEY.getKey(), "key1".getBytes()));
    headers.add(new RecordHeader(ConfiguredHeaders.NEW_PARTITION_KEY.getKey(), "key2".getBytes()));

    // when
    ProducerRecord<Object, Object> record = interceptor
        .onSend(new ProducerRecord<>("topic", null, "test", "one", headers));

    //then
    assertEquals(3, (int) record.partition());
  }

  private static List<TopicPartitionInfo> mockPartitions() {
    List<TopicPartitionInfo> partitions = new LinkedList<>();

    Node node = new Node(0, "localhost", 65535);
    for (int i = 0; i < 5; i++) {
      partitions.add(new TopicPartitionInfo(i, node, Collections.singletonList(node),
          Collections.singletonList(node)));
    }
    return partitions;
  }
}