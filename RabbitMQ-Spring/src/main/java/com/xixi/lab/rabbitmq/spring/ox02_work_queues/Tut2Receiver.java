package com.xixi.lab.rabbitmq.spring.ox02_work_queues;

import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.util.StopWatch;

/**
 * 消费者：监听队列work_queue
 */
@RabbitListener(queues = "work_queue")
public class Tut2Receiver {

    private final int instance;

    public Tut2Receiver(int i) {
        this.instance = i;
    }

    @RabbitHandler
    public void receive(String msg) throws InterruptedException {
        // StopWatch：Spring自带工具计时器
        StopWatch watch = new StopWatch();
        watch.start(); // 开始计时
        System.out.println("instance " + this.instance + " [x] Received '" + msg + "'");
        doWork(msg);
        watch.stop(); // 结束计时
        System.out.println("instance " + this.instance + " [x] Done in " + watch.getTotalTimeSeconds() + "s");
    }

    /**
     * 模拟处理消息耗时操作，一个点睡1秒
     *
     * @param msg
     * @throws InterruptedException
     */
    private void doWork(String msg) throws InterruptedException {
        for (char ch : msg.toCharArray()) {
            if (ch == '.') {
                Thread.sleep(1000);
            }
        }
    }
}
