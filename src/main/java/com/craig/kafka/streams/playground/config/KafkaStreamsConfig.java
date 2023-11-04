package com.craig.kafka.streams.playground.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
@Slf4j
public class KafkaStreamsConfig {

    @Bean
    public KStream<Integer, String> kStream(StreamsBuilder kStreamBuilder) {
        KStream<Integer, String> stream = kStreamBuilder.stream("input-topic");

        stream.mapValues(value -> value.toUpperCase())
              .peek((key, value) -> log.info("Key: {}, Value: {}", key, value))
              .to("output-topic");

        return stream;
    }

    @Bean
    public StreamsBuilderFactoryBeanCustomizer configurer() {
        return fb -> fb.setStateListener((newState, oldState) -> log.info("State transition from {} to {}", oldState, newState));
    }

//    @Bean
//    public KafkaStreamsInfrastructureCustomizer infrastructureCustomizer() {
//        return new KafkaStreamsInfrastructureCustomizer() {
//            @Override
//            public void configureBuilder(StreamsBuilder builder) {
//
//            }
//        };
//    }
}
