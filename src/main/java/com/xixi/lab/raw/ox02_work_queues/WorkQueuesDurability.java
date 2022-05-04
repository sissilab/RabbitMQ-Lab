package com.xixi.lab.raw.ox02_work_queues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * 2.2. Work Queues（Task Queues）：队列消息持久化
 *
 * @url: https://www.rabbitmq.com/tutorials/tutorial-two-java.html
 */
public class WorkQueuesDurability {

    private static final String QUEUE_NAME = "durable_queue";

    /**
     * 生产者：
     *   确保消息不会丢失需：将队列和消息都标记为持久化
     *   若该队列已存在且非持久化，则需先删除该队列，否则报错
     *   即使如此进行持久化设置，并不能完全保证消息不会丢失，因为消息在存盘时，还存在个间隔点，若这段时间RabbitMQ服务宕机、重启等异常情况，消息还未来得及存到磁盘上，此时消息则可能丢失 => 考虑镜像队列机制
     *   若将所有消息到设置为持久化，则会影响RabbitMQ的性能，毕竟写入磁盘比写入内存慢了一大截。对于可靠性不是很高的可以不采用持久化以提升整体吞吐量，故需在可靠性和吞吐量之间进行权衡
     */
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            // 声明队列时，设置队列持久化 durable=true
            boolean durable = true;
            channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
            String message = "Hello, durable queue.";
            // 发布消息时，标记消息持久化 MessageProperties.PERSISTENT_TEXT_PLAIN
            channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [x] Sent '" + message + "'");
        }
    }
}
