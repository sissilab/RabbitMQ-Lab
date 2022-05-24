package com.xixi.lab.rabbitmq.spring.ox05_topics;

import org.springframework.amqp.rabbit.annotation.RabbitListener;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Tut5Receiver {

    /**
     * 消费者1：监听 临时队列1 autoDeleteQueue1
     *
     * @param msg
     * @throws InterruptedException
     */
    @RabbitListener(queues = "#{autoDeleteQueue1.name}")
    public void receive1(String msg) throws InterruptedException {
        receive(msg, "消费者1");
    }

    /**
     * 消费者2：监听 临时队列2 autoDeleteQueue2
     *
     * @param msg
     * @throws InterruptedException
     */
    @RabbitListener(queues = "#{autoDeleteQueue2.name}")
    public void receive2(String msg) throws InterruptedException {
        receive(msg, "消费者2");
    }

    public void receive(String msg, String name) throws InterruptedException {
        System.out.printf("<<< [%s] Received: %s\n", name, msg);
        int timeCost = processMessage(msg);
        System.out.printf("[√] [%s] Done! cost = %ds\n", name, timeCost);
    }

    // 处理消息，此方法模拟随机1~3秒时间来处理
    private static int processMessage(String msg) {
        int sec = new Random().nextInt(3) + 1;
        try {
            TimeUnit.SECONDS.sleep(sec);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return sec;
    }
}