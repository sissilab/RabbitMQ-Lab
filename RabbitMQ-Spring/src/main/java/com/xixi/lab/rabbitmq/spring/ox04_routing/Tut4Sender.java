package com.xixi.lab.rabbitmq.spring.ox04_routing;

import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.PostConstruct;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * 生产者
 */
public class Tut4Sender {

    @Autowired
    private RabbitTemplate template;

    @Autowired
    private DirectExchange direct;

    private final String[] keys = {"debug", "info", "warn", "error"};

    @PostConstruct
    public void send() {
        String format = "This is %s log.";
        for (String key : keys) {
            String message = String.format(format, key);
            // 发布消息
            /* 参数1 String exchange：交换机名
             * 参数2 String routingKey：绑定键
             * 参数3 Object object：消息体
             */
            template.convertAndSend(direct.getName(), key, message);
            System.out.println(">>> Sent: " + message);
        }
    }
}