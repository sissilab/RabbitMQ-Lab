package com.xixi.lab.rabbitmq.spring.ox01_hello_world;

import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * hello-world 配置类：
 * 启动消费者：--server.port=8081 --spring.profiles.active=hello-world,receiver
 * 启动生产者：--server.port=8080 --spring.profiles.active=hello-world,sender
 *
 * @url: https://www.rabbitmq.com/tutorials/tutorial-one-spring-amqp.html
 */
@Profile("hello-world")
@Configuration
public class Tut1Config {

    /**
     * 声明一个队列 hello_queue
     * 当spring.profiles.active为hello-world，即会创建
     */
    @Bean
    public Queue hello() {
        return new Queue("hello_queue");
    }

    /**
     * 生产者
     * 当spring.profiles.active为hello-world + send，即会创建
     */
    @Profile("sender")
    @Bean
    public Tut1Sender sender() {
        return new Tut1Sender();
    }

    /**
     * 消费者
     * 当spring.profiles.active为hello-world + receive，即会创建
     */
    @Profile("receiver")
    @Bean
    public Tut1Receiver receiver() {
        return new Tut1Receiver();
    }
}
