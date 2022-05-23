package com.xixi.lab.rabbitmq.spring.ox02_work_queues;

import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Work Queues 配置类
 * 启动消费者：--server.port=8081 --spring.profiles.active=work-queues,receiver
 * 启动消费者：--server.port=8081 --spring.profiles.active=work-queues,ackReceiver
 *
 * 启动生产者：--server.port=8080 --spring.profiles.active=work-queues,sender
 *
 * @url: https://www.rabbitmq.com/tutorials/tutorial-two-spring-amqp.html
 */
@Profile("work-queues")
@Configuration
public class Tut2Config {

    public static final String QUEUE_NAME = "work-spring-queue";

    /**
     * 声明一个队列：持久化
     */
    @Bean
    public Queue hello() {
        return new Queue(QUEUE_NAME, true, false, false);
    }

    /**
     * 生产者
     */
    @Profile("sender")
    @Bean
    public Tut2Sender sender() {
        return new Tut2Sender();
    }

    /**
     * 消费者：轮询分发
     */
    @Profile("receiver")
    private static class ReceiverConfig {

        /**
         * 消费者1：模拟处理消息需2秒
         */
        @Bean
        public Tut2Receiver receiver1() {
            return new Tut2Receiver("C1", 2);
        }

        /**
         * 消费者2：模拟处理消息需6秒
         */
        @Bean
        public Tut2Receiver receiver2() {
            return new Tut2Receiver("C2", 6);
        }
    }

    /**
     * 消费者：测试消息确认
     */
    @Profile("ackReceiver")
    private static class AckReceiverConfig {

        /**
         * 消费者1：模拟处理消息需2秒
         */
        @Bean
        public AckReceiver receiver1() {
            return new AckReceiver("C1", 2);
        }

        /**
         * 消费者2：模拟处理消息需6秒
         */
        @Bean
        public AckReceiver receiver2() {
            return new AckReceiver("C2", 6);
        }
    }
}
