package com.xixi.lab.raw.ox03_publish_subscribe;

import com.rabbitmq.client.*;

import java.util.Scanner;

/**
 * 3. Publish/Subscribe 发布订阅（Fanout 扇出）：Sending messages to many consumers at once
 *
 * @desc: Distributing tasks among workers (the competing consumers pattern)
 * @url: https://www.rabbitmq.com/tutorials/tutorial-three-java.html
 * @component: 一个生产者，一个交换机（fanout），多个临时队列与多个消费者
 *
 * *Exchanges(交换机)：产者和队列之间的桥梁。生产者生产的消息不会直接发送到队列，实际上生产者不知道这些消息发送到了哪些队列中。
 *      生产者只能将消息发送到交换机中。交换机仅是：一方面接收来自生产者的消息，另一方面将这些消息推送到队列中。
 *      交换机需明确知道如何处理接收到的消息，是放入特定队列，还是丢弃它们，这些都取决于 交换机的类型。
 *  交换机的类型：直接(direct),  主题(topic) ,标题(headers) ,  扇出(fanout)
 *
 * *Temporary queues（临时队列）:创建一个具有随机名称的队列（ channel.queueDeclare().getQueue();），一旦我们断开了消费者的连接，队列将被自动删除。
 *
 * *Bindings（绑定）：exchange 和 queue 之间的桥梁，它告诉我们 exchange 和哪个队列进行了绑定关系。
 *
 * *Fanout（扇出）：它是将接收到的所有消息广播到它知道的所有队列中。
 *--------------------
 * 下面为一个的简单log系统示例：生产者发送日志信息，日志信息由生产者经由 Exchanges(交换机)，根据 Bindings（绑定） 找到合适的队列，并将日志消息放到这些队列中，再分配给各个消费者
 *      采用 扇出(fanout) 类型的交换机：实现广播消息机制，即生产者发送消息，所有订阅的消费者都会接收到消息。
 * P --> Exchange -> 临时队列1 --> C1
 *   --> Exchange -> 临时队列2 --> C2
 * 一个生产者（发送消息到交换机），一个交换机（连接2个临时队列），2个临时队列，2个消费者（每个队列对应一个消费者）
 */
public class PublishSubscribe {
}

/**
 * 生产者：发送日志消息到指定交换机（logs）中，声明交换为fanout类型（广播）
 */
class EmitLog {

    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // 声明交换机名称 + 类型(fanout)
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

            String message = "Hello World!";

            // 生产者发布消息：
            // 参数1 exchange：设置交换机名，之前为空串（Nameless exchange，默认交换机）。若exchange非空，则交由交换机来决定将消息放到哪些队列
            // 参数2 routingKey：设置为空串，之前都为队列名。若routingKey非空，则会根据此值，路由到指定队列中
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + message + "'");

            // 控制台输入消息，回车发布
            Scanner scanner = new Scanner(System.in);
            while (scanner.hasNext()) {
                String input = scanner.next();
                channel.basicPublish(EXCHANGE_NAME, "", null, input.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + input + "'");
            }
        }
    }
}

/**
 * 消费者1：接收日志消息
 * 设置绑定关系：队列名（临时队列） <--> 交换机名（logs）
 */
class ReceiveLogs1 {

    // 交换机名称
    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 声明交换机名称 + 类型(fanout)
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        // 创建临时队列，一旦我们断开了消费者的连接，队列将被自动删除
        String queueName = channel.queueDeclare().getQueue();
        // 设置Bindings关系：队列名 <--> 交换机名
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.println(" [*] 消费者1：Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] 消费者1：Received '" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}

/**
 * 消费者2：接收日志消息
 */
class ReceiveLogs2 {

    // 交换机名称
    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 声明交换机名称 + 类型(fanout)
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        // 创建临时队列，一旦我们断开了消费者的连接，队列将被自动删除
        String queueName = channel.queueDeclare().getQueue();
        // 设置Bindings关系：队列名 <--> 交换机名
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.println(" [*] 消费者2：Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] 消费者2：Received '" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}
