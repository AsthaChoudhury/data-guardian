package com.dataguardian.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DailyMetrics {
    @JsonProperty("hospital")
    private String hospital;

    @JsonProperty("date")
    private String date;

    @JsonProperty("totalRecords")
    private Long totalRecords;

    @JsonProperty("recordsWithIssues")
    private Long recordsWithIssues;

    @JsonProperty("avgQualityScore")
    private Double avgQualityScore;

    @JsonProperty("maxQualityScore")
    private Double maxQualityScore;

    @JsonProperty("minQualityScore")
    private Double minQualityScore;

    @JsonProperty("issueRate")
    private Double issueRate;

    @JsonProperty("healthStatus")
    private String healthStatus;

    @JsonProperty("variance")
    private Double variance;
}