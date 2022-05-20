package com.xixi.lab.rabbitmq.spring.ox03_publish_subscribe;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Publish/Subscribe 发布订阅 配置（fanout交换机）
 *
 * 启动消费者：--server.port=8081 --spring.profiles.active=publish-subscribe,receiver
 * 启动生产者：--server.port=8080 --spring.profiles.active=publish-subscribe,sender
 */
@Profile("publish-subscribe")
@Configuration
public class Tut3Config {

    /**
     * 创建一个fanout类型的交换机
     */
    @Bean
    public FanoutExchange fanout() {
        return new FanoutExchange("fanout_X");
    }

    /**
     * 消费者端配置
     */
    @Profile("receiver")
    private static class ReceiverConfig {

        /**
         * 创建临时队列1
         */
        @Bean
        public Queue autoDeleteQueue1() {
            // AnonymousQueue: 非持久化、排他的、自动删除 的临时队列
            return new AnonymousQueue();
        }

        /**
         * 创建临时队列2
         */
        @Bean
        public Queue autoDeleteQueue2() {
            return new AnonymousQueue();
        }

        /**
         * 建立绑定关系：临时队列autoDeleteQueue1 <--> 交换机fanout_X
         *
         * @param fanout
         * @param autoDeleteQueue1
         * @return
         */
        @Bean
        public Binding binding1(FanoutExchange fanout, Queue autoDeleteQueue1) {
            return BindingBuilder.bind(autoDeleteQueue1).to(fanout);
        }

        /**
         * 建立绑定关系：临时队列autoDeleteQueue2 <--> 交换机fanout_X
         *
         * @param fanout
         * @param autoDeleteQueue2
         * @return
         */
        @Bean
        public Binding binding2(FanoutExchange fanout, Queue autoDeleteQueue2) {
            return BindingBuilder.bind(autoDeleteQueue2).to(fanout);
        }

        /**
         * 创建消费者
         */
        @Bean
        public Tut3Receiver receiver() {
            return new Tut3Receiver();
        }
    }

    /**
     * 创建生产者
     */
    @Profile("sender")
    @Bean
    public Tut3Sender sender() {
        return new Tut3Sender();
    }
}
