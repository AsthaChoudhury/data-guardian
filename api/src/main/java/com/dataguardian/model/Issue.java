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
public class Issue {

    @JsonProperty("patientId")
    private String patientId;

    @JsonProperty("hospital")
    private String hospital;

    @JsonProperty("qualityScore")
    private Integer qualityScore;

    @JsonProperty("ageValid")
    private Boolean ageValid;

    @JsonProperty("ageValue")
    private Integer ageValue;

    @JsonProperty("bpValid")
    private Boolean bpValid;

    @JsonProperty("bpValue")
    private Integer bpValue;

    @JsonProperty("tempValid")
    private Boolean tempValid;

    @JsonProperty("tempValue")
    private Double tempValue;

    @JsonProperty("hasAnomalies")
    private Boolean hasAnomalies;

    @JsonProperty("severity")
    private String severity;

    @JsonProperty("detectedAt")
    private String detectedAt;
}