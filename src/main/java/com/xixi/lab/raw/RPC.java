package com.xixi.lab.raw;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

/**
 * 六、Remote procedure call (RPC)：Request/reply pattern example （使用一个相同队列）
 * https://www.rabbitmq.com/tutorials/tutorial-six-java.html
 *
 * 客户端和服务端 即为生成者，又为消费者。
 *
 * 客户端发布计算请求到队列中 --> 服务端接收请求，并计算处理 --> 服务端将计算结果发布到队列中 --> 客户端接收计算结果
 *
 */
public class RPC {
}

/**
 * 客户端：
 * 作为生产者，发布计算请求到队列中
 * 作为消费者，等待服务端计算完成并放入队列后，会接收返回的计算结果
 */
class RPCClient implements AutoCloseable {

    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue";

    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public static void main(String[] argv) {
        try (RPCClient fibonacciRpc = new RPCClient()) {
            for (int i = 0; i < 32; i++) {
                String i_str = Integer.toString(i);
                System.out.println(" [x] Requesting fib(" + i_str + ")");
                String response = fibonacciRpc.call(i_str);
                System.out.println(" [.] Got '" + response + "'");
            }
        } catch (IOException | TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 客户端发起计算请求，让远程服务端来处理计算该请求
     *
     * @param message
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public String call(String message) throws IOException, InterruptedException {
        // 随机生成一个 correlationId，关联请求与返回：发布给服务端时携带该值，服务端处理好再次发布返回计算结果后，根据该值来确定是哪个请求
        final String corrId = UUID.randomUUID().toString();

        // 创建临时队列，一旦我们断开了消费者的连接，队列将被自动删除
        String replyQueueName = channel.queueDeclare().getQueue();
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId) // correlationId
                .replyTo(replyQueueName) // replyTo：a callback queue
                .build();

        // >>>>>>发布：发布计算请求到队列中，让远程的服务端进行计算处理
        channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));

        final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

        // <<<<<<接收：当服务端计算完成后，会重新将结果发布到队列中，这里消费接收服务端的计算结果
        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            // 通过 correlationId 来保证此刻接收到计算结果，与当初发布的计算请求 为同一个
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response.offer(new String(delivery.getBody(), "UTF-8"));
            }
        }, consumerTag -> {
        });

        String result = response.take(); // 从队列获取数据，若为空，则一直等待，直到队列加入新数据
        channel.basicCancel(ctag);
        return result;
    }

    public void close() throws IOException {
        connection.close();
    }
}

/**
 * 服务端：
 * 作为消费者，接收来自客户端的计算请求
 * 作为生产者，计算处理完成后，将结果发布到队列中，等到客户端获取该结果
 */
class RPCServer {

    private static final String RPC_QUEUE_NAME = "rpc_queue";

    private static int fib(int n) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fib(n - 1) + fib(n - 2);
    }

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
            channel.queuePurge(RPC_QUEUE_NAME);

            channel.basicQos(1);

            System.out.println(" [x] Awaiting RPC requests");

            Object monitor = new Object();
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                        .Builder()
                        .correlationId(delivery.getProperties().getCorrelationId()) // correlationId
                        .build();

                String response = "";

                try {
                    String message = new String(delivery.getBody(), "UTF-8");
                    int n = Integer.parseInt(message);

                    System.out.println(" [.] fib(" + message + ")");
                    response += fib(n);
                } catch (RuntimeException e) {
                    System.out.println(" [.] " + e.toString());
                } finally {
                    // >>>>>>发布：服务端计算完 fib() 后，将结果 response + correlationId 发布到队列 rpc_queue 中，等待客户端接收该计算结果
                    channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                    // 手动答复消息已处理，若出现异常致使没有答复，则该消息会重新回到队列中，再重新分发下去
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    // RabbitMq consumer worker thread notifies the RPC server owner thread
                    synchronized (monitor) {
                        monitor.notify();
                    }
                }
            };

            // <<<<<<接收：接收来自客户端发过来的计算请求，在回调 deliverCallback 中去计算，并将结果再次发布到队列中，等待客户端接收该计算结果
            channel.basicConsume(RPC_QUEUE_NAME, false, deliverCallback, (consumerTag -> {
            }));
            // Wait and be prepared to consume the message from RPC client.
            while (true) {
                synchronized (monitor) {
                    try {
                        monitor.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
