package com.xixi.lab.rabbitmq.java.ox04_routing;

import com.rabbitmq.client.*;

import java.util.HashMap;
import java.util.Map;

/**
 * 4. Routing（Direct exchange）：路由
 *
 * @desc: Receiving messages selectively
 * @url: https://www.rabbitmq.com/tutorials/tutorial-four-java.html
 * @component: 一个生产者，一个交换机（direct），多个临时队列与多个消费者
 *
 * @绑定(Bindings): exchange（交换机） 和 queue（队列） 之间的桥梁，消费者端通过 channel.queueBind(队列名, 交换机名, routingKey)，
 *          这里 routingKey，亦可理解为 binding key 绑定键，它依赖于交换机的类型，若为 fanout 类型的而交换机，则会忽略该值。
 *
 * 相较于 “Publish/Subscribe” 的一个简单的日志示例，它会广播所有消息，这里在此基础上，增加一些小特性：
 *      如 只对error信息进行存储到磁盘，而不存储info或warn信息，以节省磁盘空间，对于其他所有类型日志消息依旧全部打印
 *
 * @direct: 一种交换机的类型，相较于 fanout 类型的交换机广播所有消息，direct 类型的交换机只会发布消息到它绑定的routingKey队列中,
 *          若 direct 类型的交换机的 绑定键routingKey 都一样的，则类似 fanout 的广播了
 */
public class Routing {
}

/**
 * 生产者：发送各种日志类型的消息
 * 发布消息时 channel.basicPublish()，要指定 exchange + routingKey
 */
class EmitLogDirect {

    // 交换机名
    private static final String EXCHANGE_NAME = "direct_logs_X";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            // 声明一个交换机：交换机名+direct类型
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
 * 消费者：绑定info和warn，只处理接收routingKey为info或warn的消息
 */
class ReceiveLogsDirectInfoWarn {

    private static final String EXCHANGE_NAME = "direct_logs_X";

    private static final String ROUTING_KEY = "info,warn";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 声明一个交换机：交换机名+direct类型
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        // 创建临时队列，一旦我们断开了消费者的连接，队列将被自动删除
        String queueName = channel.queueDeclare().getQueue();
        // 设置Bindings关系，一个交换机绑定一个队列，可设置多个routingKey：交换机 --routingKey1,routingKey2,...--> 队列名
        String[] routingKeyArr = ROUTING_KEY.split(",");
        for (String routingKey : routingKeyArr) {
            channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
        }

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "', printing...");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}

/**
 * 消费者：绑定error，只处理接收routingKey为error的消息
 */
class ReceiveLogsDirectError {

    private static final String EXCHANGE_NAME = "direct_logs_X";

    private static final String ROUTING_KEY = "error";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 声明一个交换机：交换机名+Direct类型
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        // 创建临时队列，一旦我们断开了消费者的连接，队列将被自动删除
        String queueName = channel.queueDeclare().getQueue();
        // 设置Bindings关系：交换机 --routingKey--> 队列名
        channel.queueBind(queueName, EXCHANGE_NAME, ROUTING_KEY);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "', saving to disk...");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}

