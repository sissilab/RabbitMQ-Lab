package com.xixi.lab;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class RabbitMQLabApplication {

    public static void main(String[] args) {
        SpringApplication.run(RabbitMQLabApplication.class, args);
    }

}
