package com.xixi.lab.spring.ox02_work_queues;

import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Work Queues 配置类
 * 启动消费者：--server.port=8081 --spring.profiles.active=work-queues,receiver
 * 启动生产者：--server.port=8080 --spring.profiles.active=work-queues,sender
 *
 * @url: https://www.rabbitmq.com/tutorials/tutorial-two-spring-amqp.html
 */
@Profile("work-queues")
@Configuration
public class Tut2Config {

    @Bean
    public Queue hello() {
        return new Queue("work_queue");
    }

    @Profile("receiver")
    private static class ReceiverConfig {

        @Bean
        public Tut2Receiver receiver1() {
            return new Tut2Receiver(1);
        }

        @Bean
        public Tut2Receiver receiver2() {
            return new Tut2Receiver(2);
        }
    }

    @Profile("sender")
    @Bean
    public Tut2Sender sender() {
        return new Tut2Sender();
    }

}
