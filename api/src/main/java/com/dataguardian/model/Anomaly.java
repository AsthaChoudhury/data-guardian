package com.dataguardian.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Anomaly {
    private String patientId;
    private String hospital;
    private Integer age;
    private Integer bpSystolic;
    private Double temperature;
    private Integer qualityScore;
    private Boolean ageValid;
    private Boolean bpValid;
    private Boolean tempValid;
    private Boolean ageAnomaly;
    private Boolean bpAnomaly;
    private LocalDateTime detectedAt;
}