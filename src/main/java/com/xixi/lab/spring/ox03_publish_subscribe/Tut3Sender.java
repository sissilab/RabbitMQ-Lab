package com.xixi.lab.spring.ox03_publish_subscribe;

import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 生产者：
 */
public class Tut3Sender {

    @Autowired
    private RabbitTemplate template;

    /**
     * fanout类型交换机
     */
    @Autowired
    private FanoutExchange fanout;

    AtomicInteger dots = new AtomicInteger(0);

    AtomicInteger count = new AtomicInteger(0);

    @Scheduled(fixedDelay = 1000, initialDelay = 500)
    public void send() {
        StringBuilder builder = new StringBuilder("Hello");
        if (dots.getAndIncrement() == 3) {
            dots.set(1);
        }
        for (int i = 0; i < dots.get(); i++) {
            builder.append('.');
        }
        builder.append(count.incrementAndGet());
        String message = builder.toString();
        // 发布消息
        /* 参数1 String exchange：交换机名，之前未指定，采用默认交换机，消息先发到交换机，交换机再发到相应队列中
         * 参数2 String routingKey：绑定键，之前为队列名，这里为空串（为了广播消息到所有队列）
         * 参数3 final Object object：消息体
         */
        template.convertAndSend(fanout.getName(), "", message);
        System.out.println(" [x] Sent '" + message + "'");
    }
}
