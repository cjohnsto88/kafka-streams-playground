package com.craig.kafka.streams.playground.config;

import com.craig.kafka.streams.playground.domain.ScoreWithPlayer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.kafka.KafkaItemReader;
import org.springframework.batch.item.kafka.builder.KafkaItemReaderBuilder;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

@Configuration
@Slf4j
public class BatchConfig {

    @Bean
    public Job job(Step step, JobRepository jobRepository) {
        return new JobBuilder("file-generator", jobRepository).start(step)
                                                              .build();
    }

    @Bean
    public Step step(KafkaItemReader<String, ScoreWithPlayer> kafkaItemReader, PlatformTransactionManager transactionManager, JobRepository jobRepository) {
        ItemProcessor<ScoreWithPlayer, ScoreWithPlayer> processor = scoreWithPlayer -> {
            log.info("Processing: {}", scoreWithPlayer);

            return scoreWithPlayer;
        };

        return new StepBuilder("file-generation-step", jobRepository).<ScoreWithPlayer, ScoreWithPlayer>chunk(100, transactionManager)
                                                                     .reader(kafkaItemReader)
                                                                     .processor(processor)
                                                                     .writer(itemWriter())
                                                                     .build();
    }

    @Bean
    public ListItemWriter<ScoreWithPlayer> itemWriter() {
        return new ListItemWriter<>();
    }

    @Bean
    public KafkaItemReader<String, ScoreWithPlayer> kafkaItemReader(KafkaProperties kafkaProperties, ObjectProvider<KafkaConnectionDetails> connectionDetails, KafkaAdmin kafkaAdmin) {
        Map<String, Object> consumerProperties = kafkaProperties.buildConsumerProperties();

        connectionDetails.ifAvailable(details -> consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, details.getConsumerBootstrapServers()));

        Properties properties = new Properties();
        properties.putAll(consumerProperties);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "batch-consumer-group");

        Map<String, TopicDescription> topics = kafkaAdmin.describeTopics("join-output");
        TopicDescription topicDescription = topics.get("join-output");

        Integer[] partitions = topicDescription.partitions().stream()
                                               .peek(p -> log.info("Partition: {}", p))
                                               .map(TopicPartitionInfo::partition)
                                               .toArray(Integer[]::new);

        return new KafkaItemReaderBuilder<String, ScoreWithPlayer>().name("file-creator")
                                                                    .consumerProperties(properties)
                                                                    .topic("join-output")
                                                                    .partitions(partitions)
                                                                    .saveState(false)
                                                                    .pollTimeout(Duration.ofSeconds(2L))
                                                                    .build();
    }


}
