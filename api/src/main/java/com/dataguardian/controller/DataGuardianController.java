package com.dataguardian.controller;

import com.dataguardian.model.DailyMetrics;
import com.dataguardian.model.Issue;
import com.dataguardian.model.Trend;
import com.dataguardian.service.MetricsService;
import lombok.RequiredArgsConstructor;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.*;

@RestController
@RequestMapping("/v1")
@CrossOrigin(origins = "*")
@RequiredArgsConstructor
public class DataGuardianController {

    private final MetricsService metricsService;

    @GetMapping("/metrics/daily/{hospital}")
    public ResponseEntity<?> getDailyMetrics(@PathVariable String hospital) {
        try {
            DailyMetrics metrics = metricsService.getDailyMetricsForHospital(hospital);
            if (metrics == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("error", "No metrics found for hospital: " + hospital));
            }

            return ResponseEntity.ok(metrics);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Error fetching daily metrics: " + e.getMessage()));
        }
    }

    @GetMapping("/metrics/status/{status}")
    public ResponseEntity<?> getMetricsByStatus(@PathVariable String status) {
        try {
            List<DailyMetrics> metrics = metricsService.getMetricsByStatus(status);
            Map<String, Object> response = new HashMap<>();
            response.put("status", status);
            response.put("count", metrics.size());
            response.put("metrics", metrics);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/metrics")
    public ResponseEntity<List<DailyMetrics>> getAllMetrics() {

        return ResponseEntity.ok(
                metricsService.getAllMetrics());
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        Map<String, Object> status = metricsService.getCurrentStatus();
        return ResponseEntity.ok(status);
    }

    @GetMapping("/metrics/critical")
    public ResponseEntity<?> getCriticalHospitals() {
        try {
            List<DailyMetrics> critical = metricsService.getCriticalHospitals();

            Map<String, Object> response = new HashMap<>();
            response.put("criticalCount", critical.size());
            response.put("hospitals", critical);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/trends/{hospital}")
    public ResponseEntity<?> getTrend(@PathVariable String hospital) {
        try {
            Trend trend = metricsService.getTrend(hospital);

            if (trend == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("error", "Trend data not found"));
            }

            return ResponseEntity.ok(trend);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/issues")
    public ResponseEntity<?> getRecentIssues(
            @RequestParam(defaultValue = "10") int limit) {

        try {
            if (limit > 100)
                limit = 100;

            List<Issue> issues = metricsService.getRecentIssues(limit);

            Map<String, Object> response = new HashMap<>();
            response.put("count", issues.size());
            response.put("issues", issues);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/issues/severity/{severity}")
    public ResponseEntity<?> getIssuesBySeverity(
            @PathVariable String severity,
            @RequestParam(defaultValue = "10") int limit) {

        try {
            if (limit > 100)
                limit = 100;

            List<Issue> issues = metricsService.getIssuesBySeverity(severity, limit);

            Map<String, Object> response = new HashMap<>();
            response.put("severity", severity);
            response.put("count", issues.size());
            response.put("issues", issues);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "DataGuardian API",
                "timestamp", new Date()));
    }

    @GetMapping("/info")
    public ResponseEntity<Map<String, String>> info() {
        return ResponseEntity.ok(Map.of(
                "name", "DataGuardian",
                "version", "1.0.0",
                "description", "Real-time data quality monitoring platform"));
    }
}