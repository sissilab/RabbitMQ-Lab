package com.xixi.lab.rabbitmq.spring.ox05_topics;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;


/**
 * Topics 主题（模式匹配）
 *
 * 启动消费者：--server.port=8081 --spring.profiles.active=topics,sender
 * 启动生产者：--server.port=8080 --spring.profiles.active=topics,receiver
 *
 * 下面构建的绑定关系：
 *      临时队列1 autoDeleteQueue1：*.orange.*、*.*.rabbit
 *      临时队列2 autoDeleteQueue2：lazy.#
 *
 */
@Profile("topics")
@Configuration
public class Tut5Config {

    /**
     * 创建一个topic类型的交换机
     */
    @Bean
    public TopicExchange topic() {
        return new TopicExchange("topic_X");
    }

    @Profile("receiver")
    private static class ReceiverConfig {

        @Bean
        public Tut5Receiver receiver() {
            return new Tut5Receiver();
        }

        @Bean
        public Queue autoDeleteQueue1() {
            return new AnonymousQueue();
        }

        @Bean
        public Queue autoDeleteQueue2() {
            return new AnonymousQueue();
        }

        /**
         * 临时队列1 autoDeleteQueue1 <--*.orange.*--> topic交换机
         */
        @Bean
        public Binding binding1a(TopicExchange topic, Queue autoDeleteQueue1) {
            return BindingBuilder.bind(autoDeleteQueue1).to(topic).with("*.orange.*");
        }

        /**
         * 临时队列1 autoDeleteQueue1 <--*.*.rabbit--> topic交换机
         */
        @Bean
        public Binding binding1b(TopicExchange topic, Queue autoDeleteQueue1) {
            return BindingBuilder.bind(autoDeleteQueue1).to(topic).with("*.*.rabbit");
        }

        /**
         * 临时队列2 autoDeleteQueue2 <--lazy.#--> topic交换机
         */
        @Bean
        public Binding binding2a(TopicExchange topic, Queue autoDeleteQueue2) {
            return BindingBuilder.bind(autoDeleteQueue2).to(topic).with("lazy.#");
        }

    }

    @Profile("sender")
    @Bean
    public Tut5Sender sender() {
        return new Tut5Sender();
    }

}