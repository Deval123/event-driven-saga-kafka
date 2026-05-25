package com.devalere.eventdriven.saga;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SagaKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(SagaKafkaApplication.class, args);
    }
}
