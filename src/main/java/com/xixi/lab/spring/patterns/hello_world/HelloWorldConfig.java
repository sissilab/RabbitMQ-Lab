package com.xixi.lab.spring.patterns.hello_world;

import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HelloWorldConfig {

    @Bean
    public Queue queue() {
        return new Queue("hh2");
    }

}
