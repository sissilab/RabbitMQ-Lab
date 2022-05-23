package com.xixi.lab.rabbitmq.spring.ox02_work_queues;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.PostConstruct;
import java.util.concurrent.atomic.AtomicInteger;

public class Tut2Sender {

    @Autowired
    private RabbitTemplate template;

    @Autowired
    private Queue queue;


    @PostConstruct
    public void send() {
        String text = "Hello ";
        for (int i = 0; i < 10; i++) {
            String message = text + i;
            //template.convertAndSend(queue.getName(), message);
            sendPersistentMsg(message);
            System.out.println(">>> Sent: " + message);
        }
    }

    /**
     * 发布消息（并持久化消息）
     *
     * @param msg
     */
    private void sendPersistentMsg(String msg) {
        template.convertAndSend(queue.getName(), (Object) msg, new MessagePostProcessor() {
            // 设置消息持久化
            @Override
            public Message postProcessMessage(Message message) throws AmqpException {
                MessageProperties props = message.getMessageProperties();
                props.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                return message;
            }
        });
    }
}
