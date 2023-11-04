package com.craig.kafka.streams.playground.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ScoreWithPlayer {

    private ScoreEvent scoreEvent;
    private Player player;

}
