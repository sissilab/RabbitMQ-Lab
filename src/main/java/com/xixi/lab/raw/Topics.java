package com.xixi.lab.raw;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.util.HashMap;
import java.util.Map;

/**
 * 五、Topics（Topic exchange，模式匹配）：Receiving messages based on a pattern (topics)
 *
 * *两个重要替换符：
 * （1）*：星号可以代替一个单词
 * （2）#：井号可以代替零个或多个单词
 *
 * 例如：<speed>.<colour>.<species>
 *     如 quick.orange.rabbit、lazy.orange.elephant、quick.orange.fox、lazy.brown.fox
 *
 * 注意：
 * 1）当一个队列绑定键是 #，那么这个队列将接收所有数据，就有点像 fanout 了
 * 2）如果队列绑定键当中没有#和*出现，那么该队列绑定类型就是 direct 了
 *
 */
public class Topics {
}

class EmitLogTopic {

    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            // 声明一个交换机：交换机名+topic类型
            channel.exchangeDeclare(EXCHANGE_NAME, "topic");

            // routingKey（pattern） -> message
            Map<String, String> bindingKeyMap = new HashMap<>();
            bindingKeyMap.put("quick.orange.rabbit", "Q1Q2接收到");
            bindingKeyMap.put("lazy.orange.elephant", "Q1Q2接收到");
            bindingKeyMap.put("quick.orange.fox", "Q1接收到");
            bindingKeyMap.put("lazy.brown.fox", "Q2接收到");
            bindingKeyMap.put("lazy.pink.rabbit","虽然满足两个绑定但只被队列 Q2 接收一次");
            bindingKeyMap.put("quick.brown.fox","不匹配任何绑定不会被任何队列接收到会被丢弃");
            bindingKeyMap.put("quick.orange.male.rabbit","是四个单词不匹配任何绑定会被丢弃");
            bindingKeyMap.put("lazy.orange.male.rabbit","是四个单词但匹配 Q2");

            for (Map.Entry<String, String> entry : bindingKeyMap.entrySet()) {
                String routingKey = entry.getKey();
                String message = entry.getValue();

                // 生产者发布消息：
                // 参数1：exchange：设置交换机名
                // 参数2：routingKey：即设置 binding key（pattern）
                channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");
            }
        }
    }
    //..
}

/**
 * 消费者：*.orange.*
 */
class ReceiveLogsTopic1 {

    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 声明一个交换机：交换机名+topic类型
        channel.exchangeDeclare(EXCHANGE_NAME, "topic");
        // 创建临时队列，一旦我们断开了消费者的连接，队列将被自动删除
        String queueName = channel.queueDeclare().getQueue();
        // 设置Bindings关系：队列名 <--> 交换机名， routingKey（pattern）
        // *.orange.*：中间为orange的3个单词
        channel.queueBind(queueName, EXCHANGE_NAME, "*.orange.*");

        System.out.println(" [*.orange.*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [*.orange.*] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}

/**
 * 消费者：*.*.rabbit, lazy.#
 */
class ReceiveLogsTopic2 {

    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 声明一个交换机：交换机名+topic类型
        channel.exchangeDeclare(EXCHANGE_NAME, "topic");
        // 创建临时队列，一旦我们断开了消费者的连接，队列将被自动删除
        String queueName = channel.queueDeclare().getQueue();
        // 设置Bindings关系：队列名 <--> 交换机名， routingKey（pattern）
        // *.*.rabbit：最后一位为rabbit的3个单词
        channel.queueBind(queueName, EXCHANGE_NAME, "*.*.rabbit");
        // lazy.#：以lazy开头的多个单词
        channel.queueBind(queueName, EXCHANGE_NAME, "lazy.#");

        System.out.println(" [*.*.rabbit, lazy.#] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [*.*.rabbit, lazy.#] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}

