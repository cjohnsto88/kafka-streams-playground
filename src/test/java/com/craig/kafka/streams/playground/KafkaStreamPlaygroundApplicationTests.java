package com.craig.kafka.streams.playground;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
		properties = {
				"spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.IntegerSerializer",
				"spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer",
				"spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.IntegerDeserializer",
				"spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
				"spring.kafka.consumer.group-id=test-assert-group"
		}
)
@Testcontainers
class KafkaStreamPlaygroundApplicationTests {

	@Container
	@ServiceConnection
	static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));

	@Autowired
	private KafkaTemplate<Integer, String> kafkaTemplate;

	@Autowired
	private ConsumerFactory<Integer, String> consumerFactory;

	@BeforeAll
	static void beforeAll() {
		NewTopic inputTopic = TopicBuilder.name("input-topic")
										  .partitions(12)
										  .build();
		NewTopic outputTopic = TopicBuilder.name("output-topic")
										  .partitions(12)
										  .build();

		KafkaAdmin kafkaAdmin = new KafkaAdmin(Map.of());
		kafkaAdmin.setBootstrapServersSupplier(() -> kafkaContainer.getBootstrapServers());
		kafkaAdmin.createOrModifyTopics(inputTopic, outputTopic);
	}

	@Test
	void contextLoads() {
		kafkaTemplate.send("input-topic", 1, "one");

		try (Consumer<Integer, String> consumer = consumerFactory.createConsumer()) {
			consumer.subscribe(List.of("output-topic"));

			ConsumerRecord<Integer, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, "output-topic");
			assertThat(singleRecord.value()).isEqualTo("ONE");
		}
	}

}
