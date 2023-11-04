package com.craig.kafka.streams.playground.config;

import com.craig.kafka.streams.playground.domain.Player;
import com.craig.kafka.streams.playground.domain.ScoreEvent;
import com.craig.kafka.streams.playground.domain.ScoreWithPlayer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
@EnableKafkaStreams
@Slf4j
public class KafkaStreamsConfig {

    @Bean
    public KStream<String, ScoreEvent> scoreEventsStream(StreamsBuilder kStreamBuilder) {
        return kStreamBuilder.stream("score-events", Consumed.with(Serdes.String(), new JsonSerde<>(ScoreEvent.class)))
                             .selectKey((key, value) -> Integer.toString(value.getPlayerId()));
    }

    @Bean
    public KTable<String, Player> playersTable(StreamsBuilder kStreamBuilder) {
        return kStreamBuilder.table("players", Consumed.with(Serdes.String(), new JsonSerde<>(Player.class)));
    }

    @Bean
    public KStream<String, ScoreWithPlayer> withPlayers(KStream<String, ScoreEvent> scoreEventsStream, KTable<String, Player> playersTable) {
        ValueJoiner<ScoreEvent, Player, ScoreWithPlayer> scorePlayerJoiner = ScoreWithPlayer::new;
        Joined<String, ScoreEvent, Player> joinParams = Joined.with(Serdes.String(), new JsonSerde<>(ScoreEvent.class), new JsonSerde<>(Player.class));

        KStream<String, ScoreWithPlayer> joinedStream = scoreEventsStream.join(playersTable, scorePlayerJoiner, joinParams)
                                                                         .toTable(Materialized.with(Serdes.String(), new JsonSerde<>(ScoreWithPlayer.class)))
                                                                         .toStream();

        joinedStream.to("join-output");

        return joinedStream;
    }

}
