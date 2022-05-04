package com.xixi.lab.spring.patterns.hello_world;

import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@RabbitListener(queuesToDeclare = @Queue(value = "hello"))
@Component
public class HelloWorldReceiver {

    @RabbitHandler
    public void receive(String message) {
        System.out.println("message = " + message);
    }

}
