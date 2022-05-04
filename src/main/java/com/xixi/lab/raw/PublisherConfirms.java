package com.xixi.lab.raw;

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
 * 七、Publisher Confirms 发布确认：Reliable publishing with publisher confirms
 * https://www.rabbitmq.com/tutorials/tutorial-seven-java.html
 * <p>
 * 生产者将信道设置成 confirm 模式，一旦信道进入 confirm 模式，所有在该信道上面发布的
 * 消息都将会被指派一个唯一的 ID(从 1 开始)，一旦消息被投递到所有匹配的队列之后，broker
 * 就会发送一个确认给生产者(包含消息的唯一 ID)，这就使得生产者知道消息已经正确到达目的队
 * 列了，如果消息和队列是可持久化的，那么确认消息会在将消息写入磁盘之后发出，broker 回传
 * 给生产者的确认消息中 delivery-tag 域包含了确认消息的序列号，此外 broker 也可以设置
 * basic.ack 的 multiple 域，表示到这个序列号之前的所有消息都已经得到了处理。
 * <p>
 * 实质上：生产者发布消息，Broker来确认消息已到队列了或写到磁盘上了
 *
 * 当生产者在发布消息时,如果交换机由于某些原因宕机或者其他原因没有收到消息或者没有将消息发送给队列时
 * 这时为了保证消息的不丢失,以便交换机恢复正常后生产者可以重新发布消息
 */
public class PublisherConfirms {

    static final int MESSAGE_COUNT = 50_000;

    static Connection createConnection() throws Exception {
        ConnectionFactory cf = new ConnectionFactory();
        cf.setHost("localhost");
        cf.setUsername("guest");
        cf.setPassword("guest");
        return cf.newConnection();
    }

    public static void main(String[] args) throws Exception {
//        publishMessagesIndividually();
//        publishMessagesInBatch();
        handlePublishConfirmsAsynchronously();
    }

    /**
     * 1、单个确认发布：同步等待确认，简单，但吞吐量非常有限
     *
     * @throws Exception
     */
    static void publishMessagesIndividually() throws Exception {
        try (Connection connection = createConnection()) {
            Channel ch = connection.createChannel();

            String queue = UUID.randomUUID().toString();
            ch.queueDeclare(queue, false, false, true, null);

            // 开启发布确认
            ch.confirmSelect();
            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                ch.basicPublish("", queue, null, body.getBytes());
                //ch.waitForConfirmsOrDie(5_000);
                boolean flag = ch.waitForConfirms();
                if (flag) {
                    System.out.println(i + ":消息发送成功");
                }
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages individually in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }

    /**
     * 批量确认发布：批量同步等待确认，简单，合理的吞吐量，一旦出现问题但很难推断出是那条消息出现了问题。
     *
     * @throws Exception
     */
    static void publishMessagesInBatch() throws Exception {
        try (Connection connection = createConnection()) {
            Channel ch = connection.createChannel();

            String queue = UUID.randomUUID().toString();
            ch.queueDeclare(queue, false, false, true, null);

            // 开启发布确认
            ch.confirmSelect();

            int batchSize = 100;
            int outstandingMessageCount = 0;

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                ch.basicPublish("", queue, null, body.getBytes());
                outstandingMessageCount++;

                if (outstandingMessageCount == batchSize) {
                    ch.waitForConfirmsOrDie(5_000);
                    outstandingMessageCount = 0;
                }
            }

            if (outstandingMessageCount > 0) {
                ch.waitForConfirmsOrDie(5_000);
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages in batch in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }

    /**
     * 异步确认发布：最佳性能和资源使用，在出现错误的情况下可以很好地控制，但是实现起来稍微难些
     *
     * @throws Exception
     */
    static void handlePublishConfirmsAsynchronously() throws Exception {
        try (Connection connection = createConnection()) {
            Channel ch = connection.createChannel();

            String queue = UUID.randomUUID().toString();
            ch.queueDeclare(queue, false, false, true, null);

            // 开启发布确认
            ch.confirmSelect();

            ConcurrentNavigableMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();

            // ack 回调
            ConfirmCallback ackCallback = (sequenceNumber, multiple) -> {
                System.out.println("ackCallback: multiple=" + multiple + ", sequenceNumber=" + sequenceNumber);
                // true 返回的是小于等于当前序列号的未确认消息；false 确认当前序列号消息
                if (multiple) {
                    // headMap(sequenceNumber): 获取key小于等于sequenceNumber的所有map集合
                    ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(sequenceNumber, true);
                    StringBuilder keySb = new StringBuilder();
                    for (Long seqNo : confirmed.keySet()) {
                        keySb.append(seqNo).append(", ");
                    }
                    System.out.println("multiple=true: sequenceNumber=" + sequenceNumber + ">>" + keySb.toString());
                    confirmed.clear();
                } else {
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

            // 添加一个异步确认监听器
            ch.addConfirmListener(ackCallback, nackCallback);

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                // channel.getNextPublishSeqNo()：获取下一个消息的序列号
                long nextPublishSeqNo = ch.getNextPublishSeqNo();
                outstandingConfirms.put(nextPublishSeqNo, body);
                System.out.println("发布：" + nextPublishSeqNo);
                ch.basicPublish("", queue, null, body.getBytes());
            }

            if (!waitUntil(Duration.ofSeconds(60), () -> outstandingConfirms.isEmpty())) {
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

