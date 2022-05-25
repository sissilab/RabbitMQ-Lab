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
 * 启动客户端：--server.port=8081 --spring.profiles.active=rpc,client
 * 启动服务单：--server.port=8080 --spring.profiles.active=rpc,server
 *
 * （1）客户端将计算请求通过 direct交换机（spring-rpc-direct-X）+绑定键（rpc） 发布出去 convertSendAndReceive()
 * （2）服务端建立了绑定关系：队列spring-rpc-request-queue <-- rpc --> direct交换机 (spring-rpc-direct-X)
 * （3）服务端监听 队列spring-rpc-request-queue，收到消息，并计算处理，最终将结果返回出去
 * （4）由于客户端使用 convertSendAndReceive() 同步发布消息，所以服务端的计算结果会直接return回客户端，此时客户端才可以发送下一条计算请求
 */
@Profile("rpc")
@Configuration
public class Tut6Config {

    public final static String EXCHANGE_NAME = "spring-rpc-direct-X";

    public final static String ROUTING_KEY = "rpc";

    public final static String REQUEST_QUEUE = "spring-rpc-request-queue";

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
            return new DirectExchange(EXCHANGE_NAME);
        }

        /**
         * 创建客户端
         */
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
        public DirectExchange exchange() {
            return new DirectExchange(EXCHANGE_NAME);
        }

        /**
         * 创建队列 spring-rpc-request-queue
         */
        @Bean
        public Queue queue() {
            return new Queue(REQUEST_QUEUE);
        }

        /**
         * 构建绑定关系：队列spring-rpc-request-queue <-- rpc --> direct交换机 (spring-rpc-direct-X)
         * 服务端通过该绑定，将客户端的计算请求发布到 队列 spring-rpc-request-queue 中
         */
        @Bean
        public Binding binding(DirectExchange exchange, Queue queue) {
            return BindingBuilder.bind(queue).to(exchange).with(ROUTING_KEY);
        }

        /**
         * 创建服务端
         */
        @Bean
        public Tut6Server server() {
            return new Tut6Server();
        }
    }
}