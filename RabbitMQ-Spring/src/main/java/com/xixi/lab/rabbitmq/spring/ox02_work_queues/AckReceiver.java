package com.xixi.lab.rabbitmq.spring.ox02_work_queues;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 消费者：监听队列
 */
@RabbitListener(queues = Tut2Config.QUEUE_NAME)
public class AckReceiver {

    private String name;

    private int time;

    public AckReceiver(String name, int time) {
        this.name = name;
        this.time = time;
    }

    @RabbitHandler
    public void receive(String msg, Channel channel, @Header(name = AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        System.out.printf("<<< [%s] Received: %s, deliveryTag=%d\n", name, msg, deliveryTag);
        processMessage(msg);
        System.out.printf(" [%s] Done!!!\n", name);

        /**
         * 当设置 spring.rabbitmq.listener.simple.acknowledge-mode: manual
         * 需手动消息确认
         */
        //channel.basicAck(deliveryTag, false);
    }

    /**
     * 模拟消息处理
     *
     * @param msg
     */
    private void processMessage(String msg) {
        // 模拟消费1的处理都是异常
        if ("C1".equals(name)) {
            /**
             * 当设置 spring.rabbitmq.listener.simple.acknowledge-mode: auto
             * 若处理方法抛出异常，则该消息会重新分发
             */
            throw new RuntimeException("simulate exception!");
        }
        try {
            TimeUnit.SECONDS.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
