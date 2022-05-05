package com.xixi.lab.raw.ox07_publisher_confirms;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BooleanSupplier;

/**
 * 7. Publisher Confirms 发送方发布确认机制
 *
 * @desc: Reliable publishing with publisher confirms
 * @url: https://www.rabbitmq.com/tutorials/tutorial-seven-java.html
 *
 * 生产者（消息发送方）不知道消息是否真正到达RabbitMQ，故这里引入 发送方发布确认机制。
 * 生产者一旦将 信道channel 设置为 确认模式（channel.confirmSelect()），所有在该 信道channel 上发布的消息都会被指派一个唯一的ID（从1开始），
 * 当消息被投递到所有匹配的d队列之后，RabbitMQ就会发送一条 确认ack 给生产者（包含被指派的ID），这样生产者就能得知消息已到达目的地。
 * 此外，若消息和队列设为持久化，那么会在消息写入磁盘后，再发出 确认ack。
 *
 * 实质上：生产者发布消息，Broker来确认消息已到队列了或写到磁盘上了
 *
 * 当生产者在发布消息时,如果交换机由于某些原因宕机或者其他原因没有收到消息或者没有将消息发送给队列时
 * 这时为了保证消息的不丢失,以便交换机恢复正常后生产者可以重新发布消息
 */
public class PublisherConfirms {

    static final int MESSAGE_COUNT = 50_000;

    static Connection createConnection() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        return connectionFactory.newConnection();
    }

    public static void main(String[] args) throws Exception {
        // 单个确认发布
        publishMessagesIndividually();
        // 批量确认发布
        publishMessagesInBatch();
        // 异步确认发布
        handlePublishConfirmsAsynchronously();
    }

    /**
     * Strategy #1: Publishing Messages Individually
     * 单个确认发布：同步等待确认，简单，但吞吐量有限，发布速度慢
     */
    static void publishMessagesIndividually() throws Exception {
        try (Connection connection = createConnection()) {
            Channel channel = connection.createChannel();

            String queue = UUID.randomUUID().toString();
            channel.queueDeclare(queue, false, false, true, null);

            // 开启发布确认
            channel.confirmSelect();
            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                channel.basicPublish("", queue, null, body.getBytes());
                //channel.waitForConfirmsOrDie(5_000); // 只有在消息被确认的时候才返回，若在指定时间内未确认则抛出异常TimeoutException
                if (channel.waitForConfirms()) { // 等到消息确认
                    System.out.println(i + ": 消息发送成功");
                }
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages individually in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }

    /**
     * Strategy #2: Publishing Messages in Batches
     * 批量确认发布：批量同步等待确认，简单，合理的吞吐量，一旦出现问题很难推断出是哪条消息出现了问题，实际仍为同步的，一样是阻塞消息的发布
     */
    static void publishMessagesInBatch() throws Exception {
        try (Connection connection = createConnection()) {
            Channel channel = connection.createChannel();

            String queue = UUID.randomUUID().toString();
            channel.queueDeclare(queue, false, false, true, null);

            // 开启发布确认
            channel.confirmSelect();

            int batchSize = 100;
            int outstandingMessageCount = 0;

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                channel.basicPublish("", queue, null, body.getBytes());
                outstandingMessageCount++;

                // 批量确认，每100条确认一次
                if (outstandingMessageCount == batchSize) {
                    channel.waitForConfirmsOrDie(5_000);
                    System.out.println("消息确认, i=" + i);
                    outstandingMessageCount = 0;
                }
            }
            // 确保剩余未确认的消息完成确认
            if (outstandingMessageCount > 0) {
                channel.waitForConfirmsOrDie(5_000);
                System.out.println("剩余所有消息确认");
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages in batch in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }

    /**
     * Strategy #3: Handling Publisher Confirms Asynchronously
     * 异步确认发布：最佳性能和资源使用，在出现错误的情况下可以很好地控制，但是实现起来稍微难些
     */
    static void handlePublishConfirmsAsynchronously() throws Exception {
        try (Connection connection = createConnection()) {
            Channel channel = connection.createChannel();

            String queue = UUID.randomUUID().toString();
            channel.queueDeclare(queue, false, false, true, null);

            // 开启发布确认
            channel.confirmSelect();

            // 存储未确认的消息，当消息被确认后，则清除掉
            // map: nextPublishSeqNo -> body(消息)
            ConcurrentNavigableMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();

            // ack 回调
            ConfirmCallback ackCallback = (sequenceNumber, multiple) -> {
                System.out.println("ackCallback: multiple=" + multiple + ", sequenceNumber=" + sequenceNumber);
                // multiple: true 返回的是小于等于当前序列号的未确认消息; false 确认当前序列号消息
                if (multiple) {
                    // headMap(sequenceNumber): 合获取key小于sequenceNumber的所有map集
                    ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(sequenceNumber, true);
                    StringBuilder keySb = new StringBuilder();
                    for (Long seqNo : confirmed.keySet()) {
                        keySb.append(seqNo).append(", ");
                    }
                    System.out.println("multiple=true: sequenceNumber=" + sequenceNumber + ">>" + keySb.toString());
                    confirmed.clear(); // 清除已确认的消息
                } else {
                    // 清除当前sequenceNumber的已确认的消息
                    outstandingConfirms.remove(sequenceNumber);
                }
            };
            // nack 回调
            ConfirmCallback nackCallback = (sequenceNumber, multiple) -> {
                System.out.println("nackCallback: multiple=" + multiple + ", sequenceNumber=" + sequenceNumber);
                String body = outstandingConfirms.get(sequenceNumber);
                System.err.format(
                        "Message with body %s has been nack-ed. Sequence number: %d, multiple: %b%n",
                        body, sequenceNumber, multiple
                );
                ackCallback.handle(sequenceNumber, multiple);
            };

            /* 添加一个异步确认监听器：
             * 参数1 ConfirmCallback ackCallback：确认消息的回调
             * 参数2 ConfirmCallback nackCallback：未收到消息的回调
             */
            channel.addConfirmListener(ackCallback, nackCallback);

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                long nextPublishSeqNo = channel.getNextPublishSeqNo(); // 获取下一个消息的序列号
                outstandingConfirms.put(nextPublishSeqNo, body);
                System.out.printf("发布: nextPublishSeqNo=%d, body=%s\n", nextPublishSeqNo, body);
                channel.basicPublish("", queue, null, body.getBytes());
            }

            // 若消息在60秒内未完全确认好，则抛出异常
            if (!waitUntil(Duration.ofSeconds(60), outstandingConfirms::isEmpty)) {
                throw new IllegalStateException("All messages could not be confirmed in 60 seconds");
            }

            long end = System.nanoTime();
            System.out.format("Published %,d messages and handled confirms asynchronously in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }

    static boolean waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        int waited = 0;
        while (!condition.getAsBoolean() && waited < timeout.toMillis()) {
            Thread.sleep(100L);
            waited = +100;
        }
        return condition.getAsBoolean();
    }

}

