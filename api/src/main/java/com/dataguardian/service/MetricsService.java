package com.dataguardian.service;

import com.dataguardian.constants.RedisKeys;
import com.dataguardian.model.DailyMetrics;
import com.dataguardian.model.Issue;
import com.dataguardian.model.Trend;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class MetricsService {

    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    public DailyMetrics getDailyMetricsForHospital(String hospital) {
        String key = String.format(RedisKeys.DAILY_METRICS, hospital);
        Object cached = redisTemplate.opsForValue().get(key);

        if (cached == null) {
            log.warn("Metrics cache miss for hospital: {}", hospital);
            return null;
        }

        return objectMapper.convertValue(cached, DailyMetrics.class);
    }

    public List<DailyMetrics> getMetricsByStatus(String status) {
        try {
            return getAllMetrics().stream()
                    .filter(m -> m.getHealthStatus().equalsIgnoreCase(status))
                    .collect(Collectors.toList());

        } catch (Exception e) {
            log.error("Error filtering by status: {}", status, e);
            return new ArrayList<>();
        }
    }

    public List<DailyMetrics> getAllMetrics() {
        Set<Object> keys = redisTemplate.opsForSet().members("dg:active_hospitals");
        if (keys == null)
            return Collections.emptyList();

        return keys.stream()
                .map(key -> redisTemplate.opsForValue().get("dg:daily_metrics:" + key.toString()))
                .filter(Objects::nonNull)
                .map(obj -> objectMapper.convertValue(obj, DailyMetrics.class))
                .collect(Collectors.toList());
    }

    public List<DailyMetrics> getCriticalHospitals() {

        try {
            return getAllMetrics().stream()
                    .filter(m -> m.getIssueRate() > 20.0 ||
                            "poor".equalsIgnoreCase(m.getHealthStatus()) ||
                            m.getAvgQualityScore() < 70.0)
                    .sorted(Comparator.comparingDouble(DailyMetrics::getIssueRate).reversed())
                    .collect(Collectors.toList());

        } catch (Exception e) {
            log.error("Error retrieving critical hospitals", e);
            return new ArrayList<>();
        }
    }

    public Trend getTrend(String hospital) {
        try {
            String key = "dg:trends:" + hospital;
            Object dataObj = redisTemplate.opsForValue().get(key);

            if (dataObj != null) {
                return objectMapper.convertValue(dataObj, Trend.class);
            }
            return Trend.builder()
                    .hospital(hospital)
                    .trendDirection("unknown")
                    .build();

        } catch (Exception e) {
            log.error("Error retrieving trend for hospital: {}", hospital, e);
            return null;
        }
    }

    public List<Issue> getRecentIssues(int limit) {
        try {
            List<Issue> issues = new ArrayList<>();

            issues.add(Issue.builder()
                    .patientId("Hospital_A_PAT_00123")
                    .hospital("Hospital_A")
                    .qualityScore(50)
                    .ageValid(false)
                    .ageValue(-5)
                    .bpValid(true)
                    .severity("high")
                    .detectedAt(new Date().toString())
                    .build());

            issues.add(Issue.builder()
                    .patientId("Hospital_B_PAT_00456")
                    .hospital("Hospital_B")
                    .qualityScore(75)
                    .bpValid(false)
                    .bpValue(300)
                    .severity("high")
                    .detectedAt(new Date().toString())
                    .build());

            return issues.stream().limit(limit).collect(Collectors.toList());

        } catch (Exception e) {
            log.error("Error retrieving recent issues", e);
            return new ArrayList<>();
        }
    }

    public Map<String, Object> getCurrentStatus() {
        try {
            String key = "dg:current_metrics";
            Object cached = redisTemplate.opsForValue().get(key);

            if (cached != null) {
                return objectMapper.convertValue(cached, Map.class);
            }

            return Map.of("status", "no_data", "issues", 0);

        } catch (Exception e) {
            return Map.of("status", "error", "message", e.getMessage());
        }
    }

    public List<Issue> getIssuesBySeverity(String severity, int limit) {
        try {
            return getRecentIssues(Integer.MAX_VALUE).stream()
                    .filter(i -> i.getSeverity().equalsIgnoreCase(severity))
                    .limit(limit)
                    .collect(Collectors.toList());

        } catch (Exception e) {
            log.error("Error retrieving issues by severity: {}", severity, e);
            return new ArrayList<>();
        }
    }
}