package com.xixi.lab.raw.ox01_hello_world;

import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;

/**
 * 1. HelloWorld
 *
 * @desc: The simplest thing that does something.
 * @url: https://www.rabbitmq.com/tutorials/tutorial-one-java.html
 * @component: 一个生产者、一个默认交换机、一个消费者
 */
public class HelloWorld {
}

/**
 * 生产者：将消息放入 hello队列 中
 */
class HelloWorldSend {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws Exception {
        // 1、创建连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        // 使用 try-with-resources 来创建 Connection，进而创建 Channel
        try (Connection connection = factory.newConnection(); // 2、通过 连接工厂 创建连接 Connection
             Channel channel = connection.createChannel()) { // 3、通过 Connection 创建信道 Channel
            // 4、利用 Channel 声明一个队列（要发送给哪个队列），指定队列名及相应参数（执行此后，即会创建好一个队列）
            /* 参数1 String queue：队列名
             * 参数2 boolean durable：true 设置队列为持久化，会存到磁盘上，MQ服务重启后不丢失；false 不持久化，默认消息存储在内存中，MQ服务重启后会丢失
             * 参数3 boolean exclusive：true 设置队列为排他的，被设置排他的队列仅对首次声明它的连接可见，并在连接断开时自动删除（即使设置了持久化）
             * 参数4 boolean autoDelete：ture 设置队列为自动删除，当至少有一个消费者已连接到该队列，之后该队列连接的所有消费者都断开时，该队列会自动删除
             * 参数5 Map<String, Object> arguments：其他参数
             */
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            String message = "Hello World!";
            // 5、发布消息（消息需转为 byte[]）至队列中
            /* 参数1 String exchange：交换机名，发布消息到该交换机中，若为空串，则发往MQ默认的交换机中
             * 参数2 String routingKey：路由键，交换机根据routingKey将消息放到对应的队列中
             * 参数3 BasicProperties props：其他属性
             * 参数4 byte[] body：消息体
             */
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [x] Sent '" + message + "'");
        }
    }
}

/**
 * 消费者：接收消息，启动后将处于一直监听消息中，当消息到达 hello队列 中，通过回调 DeliverCallback 来处理获取到的消息
 */
class HelloWorldRecv {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws Exception {
        // 1、创建连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        // 2、通过 连接工厂 创建连接 Connection
        Connection connection = factory.newConnection();
        // 3、通过 Connection 创建信道 Channel
        Channel channel = connection.createChannel();
        // 4、利用 Channel 声明一个队列（要发送给哪个队列），指定队列名及相应参数
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // DeliverCallback 用于缓存发送过来的消息，通过该回调可接收并处理消息
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.printf(" [x] Received '%s', consumerTag=%s%n", message, consumerTag);
        };
        // CancelCallback 消费取消回调
        CancelCallback cancelCallback = (consumerTag) -> {
            System.out.printf(" [x] Canceled: consumerTag=%s%n", consumerTag);
        };
        // 5、消费/接收消息，通过回调来处理接收到的消息
        /* 参数1 String queue：队列名，指定消费哪个队列
         * 参数2 boolean autoAck：true 设置为自动确认；false 设置为手动确认，利用 channel.basicAck() 进行手动确认
         * 参数3 DeliverCallback deliverCallback：接收到消息的消费回调
         * 参数4 CancelCallback cancelCallback：消费者取消时的回调（不受channel.basicCancel()影响），如队列被删除（rabbitmqctl delete_queue hello）
         */
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, cancelCallback);
    }
}
