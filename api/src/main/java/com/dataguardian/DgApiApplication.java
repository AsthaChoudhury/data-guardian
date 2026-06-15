package com.dataguardian;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;

@SpringBootApplication
@ComponentScan(basePackages = "com.dataguardian")
@EnableRedisRepositories
@EnableScheduling
public class DgApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(DgApiApplication.class, args);
        System.out.println("\n" +
                "║     DataGuardian API - Data Quality Monitoring        ║\n" +
                "║     http://localhost:8080/api/v1/metrics              ║\n");
    }
}