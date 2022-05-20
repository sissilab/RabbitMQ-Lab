package com.xixi.lab.rabbitmq.spring.ox01_hello_world;

import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;

/**
 * 消费者：接收消息
 * 通过注解 @RabbitListener 设置监听的队列
 */
@RabbitListener(queues = "hello_queue")
public class Tut1Receiver {

    /**
     * 接收消息回调
     *
     * @param msg
     */
    @RabbitHandler
    public void receive(String msg) {
        System.out.println(" [x] Received '" + msg + "'");
    }
}
