package com.xixi.lab.raw.ox02_work_queues;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 2.3. Work Queues（Task Queues） 任务分发(竞争消费者模式)：Fair dispatch
 *
 * 之前，Work Queues 针对多个消费者，都是采用 Round-robin 轮询分发消息，但是该策略在某些场景下不是很合适，如：
 *   有2个消费者C1和C2，采用轮询分发消息时，假使刚好分发的消息奇数个（C1）消息量大、处理很慢，偶数个（C2）消息量小、处理较快，
 *   此时，C2能够很快处理完，而C1还在慢慢处理，而C2处理完后并不会分担C1的任务，造成C2资源空闲，影响整体吞吐量。
 *
 * 引入 Fair Dispatch：prefetchCount(预取值) 是指单一消费者最多能消费的 unacked messages（未确认消息）数目，
 *   即通道上允许的未确认消息的最大数量，一旦超过该值，RabbitMQ 将不再往该未确认消息缓冲区中放入消息，直到该缓冲区消息被取走，小于prefetchCount。
 *
 * RabbitMQ 为每一个消费者设置一个缓冲区，大小就是 prefetchCount。每次收到一条消息，RabbitMQ 会把消息推送到缓存区中，然后再推送给客户端。
 * 假设 prefetchCount 值设为10，共有两个消费者，每个消费者每次会从队列中预抓取10条消息到本地缓存中。
 */
public class WorkQueuesFairDispatch {
}

/**
 * 生产者：生产消息，并发布到队列中
 */
class WorkQueuesFairDispatchSend {

    private final static String QUEUE_NAME = "task_queue";

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
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [x] Sent '" + message + "'");
        }
    }
}

/**
 * 消费者：
 */
class WorkQueuesFairDispatchRecv {

    private static final String QUEUE_NAME = "task_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // prefetchCount 设置为1：告诉RabbitMQ不要给某个消费者的缓冲区发送超过1条消息，打破Round-robin，处理速度快的消费者将承担更多的任务
        int prefetchCount = 1;
        channel.basicQos(prefetchCount);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
            try {
                int idx = Integer.parseInt(message.substring(message.length() - 1));
                processMessage(idx);
            } finally {
                System.out.println(" [x] Done");
                // 手动确认消息
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };
        // 设置手动确认消息 autoAck=false
        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {
        });
    }

    // 处理消息，模拟索引为偶数时处理快（2秒），为奇数时处理慢（6秒）
    private static void processMessage(int idx) {
        try {
            long time = 6;
            if (idx % 2 == 0) {
                time = 2;
            }
            TimeUnit.SECONDS.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

