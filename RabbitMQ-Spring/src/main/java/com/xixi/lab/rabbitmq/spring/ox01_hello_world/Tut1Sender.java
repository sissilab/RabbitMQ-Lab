package com.xixi.lab.rabbitmq.spring.ox01_hello_world;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalTime;

/**
 * 生产者：发送消息
 */
public class Tut1Sender {

    @Autowired
    private RabbitTemplate template;

    @Autowired
    private Queue queue;

    /**
     * 在容器启动过延迟2秒后，再每隔3秒调用发布一次消息
     */
    @Scheduled(fixedDelay = 3000, initialDelay = 2000)
    public void send() {
        String message = "Hello World! " + LocalTime.now().toString();
        template.convertAndSend(queue.getName(), message);
        System.out.println(">>> Sent: " + message);
    }
}
