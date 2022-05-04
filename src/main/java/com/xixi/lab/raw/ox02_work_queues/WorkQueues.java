package com.xixi.lab.raw.ox02_work_queues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 2.1. Work Queues（Task Queues） 任务分发(竞争消费者模式)
 *
 * @desc: Distributing tasks among workers (the competing consumers pattern)
 * @url: https://www.rabbitmq.com/tutorials/tutorial-two-java.html
 * @component: 一个生产者，一个默认的交换机，一个队列，多个消费者
 */
public class WorkQueues {
}

/**
 * 生产者：不断生产消息，并将消息发布到 hello队列 中（这里模拟循环20次来发布消息）
 */
class WorkQueuesSend {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws Exception {
        String msg = "Hello, work queues: ";
        for (int i = 0; i < 20; i++) {
            send(msg + i);
        }
    }

    private static void send(String message) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [x] Sent '" + message + "'");
        }
    }
}

/**
 * 消费者（自动确认）：接收消息，启动后将处于一直监听消息中，当消息到达队列中，通过回调 DeliverCallback 来处理获取到的消息
 *      相较于第一种（Hello World）模式，单消费者处理效率太低，故引出第二种（Work Queues）工作队列模式，面对大量资源任务时，可横向扩展消费者，并行执行任务。
 *
 * 自动确认（默认） - 顺序轮询（Round-robin dispatching）：
 *      每个消费者将固定依次被分配消息，若有2个消费者C1和C2，当发布了10个消息，C1固定被分配所有奇数的消息，C2固定被分配所有偶数的消息，
 *      且若C1的奇数消息量小（处理快），C2的偶数消息量大（处理慢），则依旧C1快速执行完自己的奇数消息，C2慢慢执行完自己的偶数消息，互不干扰，
 *      不会存在C1执行完后会替C2分担的情况。
 *
 * 额外情况：当未启动任何消费者，且生产者已发布消息到队列中时，此时启动多个消费者来接收队列中的消息，
 *          当下只会让第一个连接上的消费者来处理队列中的所有消息，其他后续连接上的消费者不会参与处理消息，
 *          当生产者继续发布消息后，后续连接上的所有消费者以及第一个消费者都会被依次分配消息。
 */
class WorkQueuesAutoAckRecv {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" + message + "'");
            try {
                processMessage();
            } finally {
                System.out.println(" [x] Done");
            }
        };
        boolean autoAck = true; // 自动确认
        channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback, consumerTag -> {
        });
    }

    // 处理消息，此方法模拟需2秒时间来处理
    private static void processMessage() {
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

/**
 * 消费者（手动确认）：消费者设置手动确认消息，当消息到达某消费者，若由于程序异常（如连接断开）未确认，该消息将会重新回到队列，
 *                  然后再次重新分发下去，当消息真正确认（ack）后，才会删除该消息。
 *
 * 为保证消息在发送过程中不丢失，RabbitMQ引入了消息确认机制：消费者在接收到消息后，告诉RabbitMQ已经被处理了，RabbitMQ可以删除该消息了。
 * 自动答复：消息发送后立即被认为已经传送成功，这种模式需要在高吞吐量和数据传输安全性（若消费者异常关闭，那么消息也会丢失）方面做权衡。
 * https://www.rabbitmq.com/confirms.html
 *
 * 数据测试结果：当有两个消费者C1和C2，C2执行过程中突然断开，则C2分配的消息全都转到C1的末尾去执行，需等C1之前分配的消息执行完，才处理C2的
 */
class WorkQueuesManualAckRecv {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // DeliverCallback 用于缓存发送过来的消息，通过该回调可接收并处理消息
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" + message + "'");
            try {
                processMessage();
            } finally {
                System.out.println(" [x] Done! DeliveryTag=" + delivery.getEnvelope().getDeliveryTag());
                // 手动确认消息已处理，若出现异常致使没有答复，则该消息会重新回到队列中，再重新分发下去
                /* 参数1 long deliveryTag：可看作消息编号，是一个64位的长整型值
                 * 参数2 boolean multiple：true 批量确认当前deliveryTag及其之前的所有消息；false 仅确认当前deliveryTag的消息
                 */
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };
        // 设置取消自动确认，需手动确认 channel.basicAck()
        // 若某个消费者接收到消息，一直未答复，超时（默认30分钟），则该Channel会关闭（PRECONDITION_FAILED） https://www.rabbitmq.com/consumers.html#acknowledgement-timeout
        boolean autoAck = false;
        channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback, consumerTag -> {
        });
    }

    // 处理消息，此方法模拟需2秒时间来处理
    private static void processMessage() {
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
