package com.xixi.lab.rabbitmq.java.ox05_topics;

import com.rabbitmq.client.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 5. Topics（Topic exchange）：模式匹配
 *
 * @desc: Receiving messages based on a pattern (topics)
 * @url: https://www.rabbitmq.com/tutorials/tutorial-five-java.html
 * @component: 一个生产者，一个交换机（topic），多个临时队列与多个消费者
 *
 * *两个重要替换符：
 * （1）*：星号可以代替一个单词
 * （2）#：井号可以代替零个或多个单词
 *
 * 例如定义：<speed>.<colour>.<species>
 *     如 quick.orange.rabbit、lazy.orange.elephant、quick.orange.fox、lazy.brown.fox
 *     *.orange.*：共3个单词，中间为orange，第一个和最后一个为任一单词，quick.orange.rabbit、lazy.orange.elephant、quick.orange.fox
 *     *.*.rabbit：共3个单词，最后一个为rabbit，前2个为任一单词，quick.orange.rabbit
 *     lazy.#：以lazy开头的所有消息，lazy.orange.elephant、lazy.brown.fox
 *
 * 注意：
 * 1）当一个队列绑定键是 #，那么这个队列将接收所有数据，类似 fanout
 * 2）如果队列绑定键当中没有#和*出现，那么该队列绑定类型就类似 direct
 */
public class Topics {
}

/**
 * 生产者：
 */
class EmitLogTopic {

    private static final String EXCHANGE_NAME = "topic_logs_X";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            // 声明一个交换机：交换机名+topic类型
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

            // routingKey（pattern） -> message
            Map<String, String> bindingKeyMap = new HashMap<>();
            bindingKeyMap.put("quick.orange.rabbit", "This is a quick and orange rabbit.");
            bindingKeyMap.put("lazy.orange.elephant", "This is a lazy and orange rabbit.");
            bindingKeyMap.put("quick.orange.fox", "This is a quick and orange fox.");
            bindingKeyMap.put("lazy.brown.fox", "This is a lazy and brown fox.");

            for (Map.Entry<String, String> entry : bindingKeyMap.entrySet()) {
                String routingKey = entry.getKey();
                String message = entry.getValue();

                // 生产者发布消息：
                // 参数1 String exchange：设置交换机名
                // 参数2 String routingKey：即设置 binding key（pattern）
                channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
                System.out.println(">>> Sent '" + routingKey + "':'" + message + "'");
            }
        }
    }
}

/**
 * 消费者：匹配接收routingKey为 “*.orange.*” 的消息
 */
class ReceiveLogsTopic1 {

    private static final String EXCHANGE_NAME = "topic_logs_X";

    // *.orange.*：中间为orange的3个单词
    private static final String ROUTING_KEY = "*.orange.*";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 声明一个交换机：交换机名+topic类型
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        // 创建临时队列，一旦我们断开了消费者的连接，队列将被自动删除
        String queueName = channel.queueDeclare().getQueue();
        // 设置Bindings关系：交换机 --routingKey--> 队列名
        channel.queueBind(queueName, EXCHANGE_NAME, ROUTING_KEY);

        System.out.println(" [*.orange.*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("<<< [*.orange.*] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}

/**
 * 消费者：匹配接收routingKey为 “*.*.rabbit, lazy.#” 的消息
 */
class ReceiveLogsTopic2 {

    private static final String EXCHANGE_NAME = "topic_logs_X";

    // *.*.rabbit：共3个单词，最后一位为rabbit
    // lazy.#：以lazy开头的单词，不限位数
    private static final List<String> ROUTING_KEYS = Arrays.asList("*.*.rabbit", "lazy.#");

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 声明一个交换机：交换机名+topic类型
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        // 创建临时队列，一旦我们断开了消费者的连接，队列将被自动删除
        String queueName = channel.queueDeclare().getQueue();
        // 设置Bindings关系：交换机 --routingKey--> 队列名
        for (String routingKey : ROUTING_KEYS) {
            channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
        }

        System.out.printf("[%s] Waiting for messages. To exit press CTRL+C\n", String.join(",", ROUTING_KEYS));

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.printf("<<< [%s] Received: routingKey=%s, message=%s\n", String.join(",", ROUTING_KEYS), delivery.getEnvelope().getRoutingKey(), message);
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }
}

