package com.xixi.lab.rabbitmq.spring.ox02_work_queues;

import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.util.StopWatch;

import java.util.concurrent.TimeUnit;

/**
 * 消费者：监听队列
 */
@RabbitListener(queues = Tut2Config.QUEUE_NAME)
public class Tut2Receiver {

    private String name;

    private int time;

    public Tut2Receiver(String name, int time) {
        this.name = name;
        this.time = time;
    }

    @RabbitHandler
    public void receive(String msg) {
        System.out.printf("<<< [%s] Received: %s\n", name, msg);
        processMessage(msg);
        System.out.printf(" [%s] Done!!!\n", name);
    }

    // 处理消息，模拟消息处理
    private void processMessage(String msg) {
        try {
            TimeUnit.SECONDS.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
