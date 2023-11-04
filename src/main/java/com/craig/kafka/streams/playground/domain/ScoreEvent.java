package com.craig.kafka.streams.playground.domain;

import lombok.Data;

@Data
public class ScoreEvent {

    private int playerId;
    private int score;

}
