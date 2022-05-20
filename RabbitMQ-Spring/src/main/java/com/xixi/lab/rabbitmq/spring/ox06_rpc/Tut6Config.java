package com.xixi.lab.rabbitmq.spring.ox06_rpc;


import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * RPC
 *
 * 启动消费者：--server.port=8081 --spring.profiles.active=rpc,sender
 * 启动生产者：--server.port=8080 --spring.profiles.active=rpc,receiver
 */
@Profile("rpc")
@Configuration
public class Tut6Config {

    /**
     * 客户端配置
     */
    @Profile("client")
    private static class ClientConfig {

        /**
         * 客户端往direct交换机(routingKey=rpc)发送计算请求
         */
        @Bean
        public DirectExchange exchange() {
            return new DirectExchange("rpc_direct_X");
        }

        @Bean
        public Tut6Client client() {
            return new Tut6Client();
        }

    }

    /**
     * 服务端配置
     */
    @Profile("server")
    private static class ServerConfig {

        @Bean
        public Queue queue() {
            return new Queue("rpc_request_queue");
        }

        @Bean
        public DirectExchange exchange() {
            return new DirectExchange("rpc_direct_X");
        }

        /**
         * 构建绑定关系：队列rpc_queue <--rpc--> direct交换机
         * 服务端通过该绑定，将客户端的计算请求发布到 队列rpc_queue 中
         */
        @Bean
        public Binding binding(DirectExchange exchange, Queue queue) {
            return BindingBuilder.bind(queue).to(exchange).with("rpc");
        }

        @Bean
        public Tut6Server server() {
            return new Tut6Server();
        }

    }

}