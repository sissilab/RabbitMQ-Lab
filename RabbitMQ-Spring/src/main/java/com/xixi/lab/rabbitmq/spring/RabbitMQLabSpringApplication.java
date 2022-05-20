package com.xixi.lab.rabbitmq.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class RabbitMQLabSpringApplication {

    public static void main(String[] args) {
        SpringApplication.run(RabbitMQLabSpringApplication.class, args);
    }

}
