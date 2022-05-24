package com.xixi.lab.rabbitmq.java.ox03_publish_subscribe;

import com.rabbitmq.client.*;

import java.util.Scanner;

/**
 * 3. Publish/Subscribe 发布订阅（Fanout 扇出）：Sending messages to many consumers at once
 *
 * @desc: Distributing tasks among workers (the competing consumers pattern)
 * @url: https://www.rabbitmq.com/tutorials/tutorial-three-java.html
 * @component: 一个生产者，一个交换机（fanout），多个临时队列与多个消费者
 *
 * @交换机(Exchanges): 生产者生产消息不会直接发到队列，生产者也不知道这些消息究竟发给了哪些队列，而生产者只能将消息发给交换机。
 *          交换机是生产者和队列之间的桥梁，一方面接收来自生产者的消息，另一方面则将这些消息推送到队列中。
 *          交换机需明确知道如何处理接收到的消息，是放入特定队列，还是丢弃它们，这些都取决于 交换机的类型。
 *          交换机的类型：fanout(扇出)、direct(直接)、topic(主题)、headers(标题)
 *
 * @临时队列: Temporary queues，创建一个具有随机名称的队列（channel.queueDeclare().getQueue()），一旦断开了消费者的连接，该临时队列将被自动删除
 *
 * @绑定(Bindings): exchange（交换机） 和 queue（队列） 之间的桥梁，它告诉我们 交换机 和 哪个队列 建立绑定关系
 *
 * @Fanout(扇出): 一种交换机的类型，它是将接收到的所有消息广播到它知道的所有队列中（存在绑定关系）
 *--------------------
 * 下面为一个的简单log日志系统示例：生产者发送日志信息，日志信息由生产者经由 Exchanges(交换机)，根据 Bindings（绑定） 找到合适的队列，并将日志消息放到这些队列中，再分配给各个消费者
 *      采用 扇出(fanout) 类型的交换机：实现广播消息机制，即生产者发送消息，所有订阅的消费者都会接收到消息。
 * P --> Exchange -> 临时队列1 --> C1
 *   --> Exchange -> 临时队列2 --> C2
 * 一个生产者（发送消息到交换机），一个fanout交换机（连接2个临时队列），2个临时队列，2个消费者（每个队列对应一个消费者）
 */
public class PublishSubscribe {
}

/**
 * 生产者：发送日志消息到指定交换机（logs_X）中，声明交换为fanout类型（广播）
 * fanout类型的交换机会将接收到的所有消息广播到所有与之绑定的队列中
 */
class EmitLog {

    private static final String EXCHANGE_NAME = "logs_X";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // 声明交换机名称 + 类型(fanout)
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

            String message = "Hello World!";

            // 生产者发布消息：
            // 参数1 String exchange：设置交换机名，之前为空串（Nameless exchange，默认交换机）。若exchange非空，则交由交换机来决定将消息放到哪些队列
            // 参数2 String routingKey：设置为空串，之前都为队列名，fanout类型交换机会忽略该值。若routingKey非空，则会根据此值，路由到指定队列中
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
            System.out.println(">>> Sent: " + message);

            // 控制台输入消息，回车发布
            Scanner scanner = new Scanner(System.in);
            while (scanner.hasNext()) {
                String input = scanner.next();
                channel.basicPublish(EXCHANGE_NAME, "", null, input.getBytes("UTF-8"));
                System.out.println(">>> Sent: " + input);
            }
        }
    }
}

/**
 * 消费者1：接收日志消息
 * 设置绑定关系：队列名（临时队列） <--> 交换机名（logs_X）
 * 类似于 消费者1的临时队列 订阅了 交换机名（logs_X），一旦该交换机接收到生产者发布的消息，都会分发到消费者1的队列
 */
class ReceiveLogs1 {

    // 交换机名称
    private static final String EXCHANGE_NAME = "logs_X";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 声明交换机名称 + 类型(fanout)
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        // 创建临时队列，一旦我们断开了消费者的连接，队列将被自动删除
        String queueName = channel.queueDeclare().getQueue();
        // 设置Bindings关系：队列名 <--> 交换机名（routingKey为空串，fanout类型交换机会忽略该值）
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.printf(" [*] 消费者1 (%s)：Waiting for messages. To exit press CTRL+C\n", queueName);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("<<< 消费者1：Received: " + message);
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}

/**
 * 消费者2：接收日志消息
 * 设置绑定关系：队列名（临时队列） <--> 交换机名（logs_X）
 * 类似于 消费者2的临时队列 也订阅了 交换机名（logs_X），一旦该交换机接收到生产者发布的消息，也都会分发到消费者2的队列
 */
class ReceiveLogs2 {

    // 交换机名称
    private static final String EXCHANGE_NAME = "logs_X";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 声明交换机名称 + 类型(fanout)
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        // 创建临时队列，一旦我们断开了消费者的连接，队列将被自动删除
        String queueName = channel.queueDeclare().getQueue();
        // 设置Bindings关系：队列名 <--> 交换机名（routingKey为空串，fanout类型交换机会忽略该值）
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.printf(" [*] 消费者2 (%s)：Waiting for messages. To exit press CTRL+C\n", queueName);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("<<< 消费者2：Received: " + message);
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}
