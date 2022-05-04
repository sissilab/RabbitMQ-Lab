package com.xixi.lab.raw;

import com.rabbitmq.client.*;

import java.util.HashMap;
import java.util.Map;

/**
 * 四、Routing（Direct exchange）：Receiving messages selectively
 * https://www.rabbitmq.com/tutorials/tutorial-four-java.html
 *
 * 相较于 “PublishSubscribe” 的一个简单的日志示例，它会广播所有消息，这里在此基础上，增加一些小特性：
 *      如 只对error信息进行存储到磁盘，而不存储info或warn信息，以节省磁盘空间，对于其他所有类型日志消息依旧全部打印
 *
 * *Direct exchange：
 * *Multiple bindings：类似fanout，广播所有消息
 *
 */
public class Routing {
}

/**
 * 生产者：发送各种日志类型的消息
 * 发布消息时 channel.basicPublish()，要指定 exchange + routingKey
 */
class EmitLogDirect {

    // 交换机名
    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            // 声明一个交换机：交换机名+Direct类型
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

            // routingKey 日志级别 -> message
            Map<String, String> bindingKeyMap = new HashMap<>();
            bindingKeyMap.put("debug", "This a debug msg.");
            bindingKeyMap.put("info", "This a info msg.");
            bindingKeyMap.put("warn", "This a warn msg.");
            bindingKeyMap.put("error", "This a error msg.");

            for (Map.Entry<String, String> entry : bindingKeyMap.entrySet()) {
                String routingKey = entry.getKey();
                String message = entry.getValue();

                // 生产者发布消息：
                // 参数1：exchange：设置交换机名
                // 参数2：routingKey：即设置 binding key
                channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");
            }
        }
    }
}

/**
 * 消费者：info
 * 设置Bindings关系时 channel.queueBind()，指定 队列名 + 交换机名 + routingKey
 */
class ReceiveLogsDirectInfo {

    private static final String EXCHANGE_NAME = "direct_logs";

    private static final String ROUTING_KEY = "info";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 声明一个交换机：交换机名+Direct类型
        channel.exchangeDeclare(EXCHANGE_NAME, "direct");
        // 创建临时队列，一旦我们断开了消费者的连接，队列将被自动删除
        String queueName = channel.queueDeclare().getQueue();
        // 设置Bindings关系：队列名 <--> 交换机名， routingKey
        channel.queueBind(queueName, EXCHANGE_NAME, ROUTING_KEY);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] [INFO] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}

/**
 * 消费者：error, warn
 */
class ReceiveLogsDirectErrorWarn {

    private static final String EXCHANGE_NAME = "direct_logs";

    private static final String ROUTING_KEY = "error,warn";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 声明一个交换机：交换机名+Direct类型
        channel.exchangeDeclare(EXCHANGE_NAME, "direct");
        // 创建临时队列，一旦我们断开了消费者的连接，队列将被自动删除
        String queueName = channel.queueDeclare().getQueue();
        // 设置Bindings关系：队列名 <--> 交换机名， routingKey
        String[] routingKeyArr = ROUTING_KEY.split(",");
        for (String routingKey : routingKeyArr) {
            channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
        }

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] [ERROR/WARN] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}

