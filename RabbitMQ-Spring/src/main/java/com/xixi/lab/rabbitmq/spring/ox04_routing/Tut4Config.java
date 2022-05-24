package com.xixi.lab.rabbitmq.spring.ox04_routing;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;


/**
 * Routing 路由 配置：direct交换机
 *
 * 启动消费者：--server.port=8081 --spring.profiles.active=routing,receiver
 * 启动生产者：--server.port=8080 --spring.profiles.active=routing,sender
 *
 * 下面构建的绑定关系：
 *      临时队列1 autoDeleteQueue1 只处理 info、warn、error
 *      临时队列2 autoDeleteQueue2 只处理 error
 *      debug 无人绑定，日志直接丢弃
 */
@Profile("routing")
@Configuration
public class Tut4Config {

    /**
     * 创建一个direct类型的交换机
     */
    @Bean
    public DirectExchange direct() {
        return new DirectExchange("spring-direct-X");
    }

    /**
     * 生产者
     */
    @Profile("sender")
    @Bean
    public Tut4Sender sender() {
        return new Tut4Sender();
    }

    /**
     * 消费者端的配置
     */
    @Profile("receiver")
    private static class ReceiverConfig {

        /**
         * 创建一个临时队列1
         */
        @Bean
        public Queue autoDeleteQueue1() {
            return new AnonymousQueue();
        }

        /**
         * 创建一个临时队列2
         */
        @Bean
        public Queue autoDeleteQueue2() {
            return new AnonymousQueue();
        }

        /**
         * 构建绑定关系：临时队列1 autoDeleteQueue1 <--info--> direct交换机
         */
        @Bean
        public Binding binding1Info(DirectExchange direct, Queue autoDeleteQueue1) {
            return BindingBuilder.bind(autoDeleteQueue1).to(direct).with("info");
        }

        /**
         * 构建绑定关系：临时队列1 autoDeleteQueue1 <--warn--> direct交换机
         */
        @Bean
        public Binding binding1Warn(DirectExchange direct, Queue autoDeleteQueue1) {
            return BindingBuilder.bind(autoDeleteQueue1).to(direct).with("warn");
        }

        /**
         * 构建绑定关系：临时队列1 autoDeleteQueue1 <--error--> direct交换机
         */
        @Bean
        public Binding binding1Error(DirectExchange direct, Queue autoDeleteQueue1) {
            return BindingBuilder.bind(autoDeleteQueue1).to(direct).with("error");
        }

        /**
         * 构建绑定关系：临时队列2 autoDeleteQueue2 <--error--> direct交换机
         */
        @Bean
        public Binding binding2Error(DirectExchange direct, Queue autoDeleteQueue2) {
            return BindingBuilder.bind(autoDeleteQueue2).to(direct).with("error");
        }

        /**
         * 消费者
         */
        @Bean
        public Tut4Receiver receiver() {
            return new Tut4Receiver();
        }
    }
}