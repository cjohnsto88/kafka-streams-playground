package com.craig.kafka.streams.playground;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
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
import org.testcontainers.shaded.org.awaitility.Durations;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@SpringBootTest(
    properties = {
        "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
        "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer",
        "spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
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
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    @BeforeAll
    static void beforeAll() {
        NewTopic[] topics = {
            TopicBuilder.name("players")
                        .partitions(3)
                .build(),
            TopicBuilder.name("score-events")
                        .partitions(3)
                .build(),
            TopicBuilder.name("join-output")
                        .partitions(3)
                        .compact()
                .build()
        };

        KafkaAdmin kafkaAdmin = new KafkaAdmin(Map.of());
        kafkaAdmin.setBootstrapServersSupplier(() -> kafkaContainer.getBootstrapServers());
        kafkaAdmin.createOrModifyTopics(topics);
    }

    @Test
    void joiningStreamToTable() throws JSONException {
        // language=json
        var playerJson = """
            {
                "id": 1,
                "firstName": "Craig"
            }
            """;

        // language=json
        var firstScoreJson = """
            {
                "playerId": 1,
                "score": 25
            }
            """;

        // language=json
        var secondScoreJson = """
            {
                "playerId": 1,
                "score": 75
            }
            """;

        kafkaTemplate.send("players", "1", playerJson);

        kafkaTemplate.send("score-events", "craig's-score", firstScoreJson);
        kafkaTemplate.send("score-events", "craig's-score", secondScoreJson);

        // language=json
        var expectedJson = """
            {
                "scoreEvent": {
                    "playerId": 1,
                    "score": 75
                },
                "player": {
                    "id": 1,
                    "firstName": "Craig"
                }
            }
            """;

        try (Consumer<String, String> consumer = consumerFactory.createConsumer()) {
            consumer.subscribe(List.of("join-output"));

            await().atMost(Durations.FIVE_SECONDS)
                   .untilAsserted(() -> {
                       ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer);

                       assertThat(records).last().satisfies(record -> JSONAssert.assertEquals(expectedJson, record.value(), JSONCompareMode.STRICT));
                   });
        }

    }
}
