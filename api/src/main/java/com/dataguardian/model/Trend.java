package com.dataguardian.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Trend {

    @JsonProperty("hospital")
    private String hospital;

    @JsonProperty("date")
    private String date;

    @JsonProperty("dailyScore")
    private Double dailyScore;

    @JsonProperty("ma7Score")
    private Double ma7Score;

    @JsonProperty("trendDirection")
    private String trendDirection;

    @JsonProperty("qualityChange")
    private Double qualityChange;

    @JsonProperty("predictionRisk")
    private Double predictionRisk;

    @JsonProperty("recommendation")
    private String recommendation;
}