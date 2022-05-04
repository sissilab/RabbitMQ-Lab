package com.xixi.lab.spring.patterns.hello_world;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class HelloWorldSender {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private Queue queue;


    public void send(String msg) {
        rabbitTemplate.convertAndSend(queue.getName(), msg);
    }

}
