package com.craig.kafka.streams.playground.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ScoreWithPlayer {

    private ScoreEvent scoreEvent;
    private Player player;

}
