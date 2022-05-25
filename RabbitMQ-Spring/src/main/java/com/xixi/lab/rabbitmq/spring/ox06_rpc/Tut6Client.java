package com.xixi.lab.rabbitmq.spring.ox06_rpc;

import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.StopWatch;

/**
 * 客户端
 */
public class Tut6Client {

    @Autowired
    private RabbitTemplate template;

    @Autowired
    private DirectExchange exchange;

    private int start = 0;

    @Scheduled(fixedDelay = 1000, initialDelay = 500)
    public void send() {
        System.out.println(">>> [C] Requesting fib(" + start + ")...");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        // 发布消息
        Integer response = (Integer) template.convertSendAndReceive(exchange.getName(), Tut6Config.ROUTING_KEY, start++);
        stopWatch.stop();
        System.out.printf("<<< [C] Got: response=%d, cost=%fs\n\n", response, stopWatch.getTotalTimeSeconds());
    }
}