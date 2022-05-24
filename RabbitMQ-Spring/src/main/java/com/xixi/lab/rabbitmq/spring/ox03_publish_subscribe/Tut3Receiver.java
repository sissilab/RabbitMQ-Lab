package com.xixi.lab.rabbitmq.spring.ox03_publish_subscribe;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.util.StopWatch;

import java.sql.SQLOutput;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 消费者：
 */
public class Tut3Receiver {

    /**
     * 消费者1 接收消息：监听临时队列1 autoDeleteQueue1
     *
     * @param msg
     * @throws InterruptedException
     */
    @RabbitListener(queues = "#{autoDeleteQueue1.name}")
    public void receive1(String msg) throws InterruptedException {
        receive(msg, "消费者1");
    }

    /**
     * 消费者2 接收消息：监听临时队列2 autoDeleteQueue2
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

    // 处理消息，此方法模拟随机1~5秒时间来处理
    private static int processMessage(String msg) {
        int sec = new Random().nextInt(5) + 1;
        try {
            TimeUnit.SECONDS.sleep(sec);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return sec;
    }
}
