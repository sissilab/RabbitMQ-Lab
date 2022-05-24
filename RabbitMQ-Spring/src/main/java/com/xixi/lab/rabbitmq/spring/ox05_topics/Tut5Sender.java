package com.xixi.lab.rabbitmq.spring.ox05_topics;


import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;


/**
 * 生产者
 */
public class Tut5Sender {

    @Autowired
    private RabbitTemplate template;

    @Autowired
    private TopicExchange topic;


    private final String[] keys = {"quick.orange.rabbit", "lazy.orange.elephant", "quick.orange.fox",
            "lazy.brown.fox", "lazy.pink.rabbit", "quick.brown.fox"};

    @PostConstruct
    public void send() {
        String format = "Hello, ";
        for (String key : keys) {
            String message = format + key;
            template.convertAndSend(topic.getName(), key, message);
            System.out.println(">>> Sent: " + message);
        }
    }
}
