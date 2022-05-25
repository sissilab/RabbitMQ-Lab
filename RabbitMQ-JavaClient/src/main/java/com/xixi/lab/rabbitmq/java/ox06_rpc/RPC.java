package com.xixi.lab.rabbitmq.java.ox06_rpc;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

/**
 * 6. Remote procedure call (RPC)：远程过程调用，通过网络从远程计算机上请求服务，通俗来说，
 *          假设有两台服务器A和B，现在服务器A上的应用想调用服务器B上的接口或函数，由于不处于同一个内存空间，无法直接调用，需通过网络来实现调用。
 *
 * @desc: Request/reply pattern example
 * @url: https://www.rabbitmq.com/tutorials/tutorial-six-java.html
 *
 * 这里使用RabbitMQ来实现RPC：客户端作为生产者通过队列rpc_queue发送请求计算的请求 --> 服务端作为队列rpc_queue的消费者，一旦接收到消息，就开始计算，
 *      当计算完成后，通过客户端创建并设置的临时队列（replyTo），此时服务端作为临时队列（从replyTo获取临时队列名）的生产者，将计算结果发布出去
 *      --> 客户端作为临时队列的消费者，监听服务端返回的计算结果，拿到计算结果后，即完成一整个RPC过程
 *
 * 可看到，客户端和服务端 即为生产者，又为消费者
 */
public class RPC {
}

/**
 * 客户端：
 * 作为生产者，发布计算请求到队列（rpc_queue）中
 * 作为消费者，等待服务端计算完成并放入队列（临时队列）后，会接收返回的计算结果（创建临时队列，并设置到replyTo传递给服务端）
 */
class RPCClient implements AutoCloseable {

    private Connection connection;
    private Channel channel;
    private String QUEUE_NAME = "rpc_queue";

    // 初始化好 连接Connection 和 信道Channel
    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public static void main(String[] argv) {
        try (RPCClient rpcClient = new RPCClient()) {
            for (int i = 0; i < 32; i++) {
                String num = Integer.toString(i);
                System.out.println(">>> [C] Requesting fib(" + num + ")......");
                String response = rpcClient.call(num);
                System.out.printf("<<< [C] Got: num=%s, response=%s\n\n", num, response);
            }
        } catch (IOException | TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 客户端发起计算请求，让远程服务端来处理计算该请求
     *
     * @param numMsg
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public String call(String numMsg) throws IOException, InterruptedException {
        // 随机生成一个 correlationId，关联请求与返回：发布给服务端时携带该值，服务端处理好再次发布返回计算结果后，根据该值来确定是哪个请求
        final String corrId = UUID.randomUUID().toString();

        // 创建临时队列，一旦我们断开了消费者的连接，队列将被自动删除（auto-delete=true、exclusive=true）
        String replyQueueName = channel.queueDeclare().getQueue();
        // 构建基本属性
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId) // correlationId
                .replyTo(replyQueueName) // replyTo: a callback queue
                .build();

        System.out.printf(">>> [C] Publish: corrId=%s, replyQueueName=%s\n", corrId, replyQueueName);
        // >>>>>>1.发布：发布计算请求到队列 rpc_queue 中，让远程的服务端通过该队列取到数据并进行计算处理
        channel.basicPublish("", QUEUE_NAME, props, numMsg.getBytes("UTF-8"));

        // 构建一个容器为1的阻塞队列
        final BlockingQueue<String> responseQueue = new ArrayBlockingQueue<>(1);

        // <<<<<<接收：当服务端计算完成后，会重新将结果发布到队列中，这里消费接收服务端的计算结果
        String ctag = channel.basicConsume(replyQueueName, true,
                // DeliverCallback
                (consumerTag, delivery) -> {
                    // 4. 客户端作为随机队列的消费者，监听服务端返回计算结果的回调
                    // 通过 correlationId 来保证此刻接收到计算结果，与当初发布的计算请求 为同一个
                    if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                        // 将计算结果加入阻塞队列
                        responseQueue.offer(new String(delivery.getBody(), "UTF-8"));
                    }
                },
                // CancelCallback
                consumerTag -> {
                });
        System.out.println("ctag=" + ctag);

        // 从队列获取数据，若为空，则一直等待（阻塞），直到队列添加了一条数据
        String result = responseQueue.take();
        channel.basicCancel(ctag); // 显示地取消客户端消费者的订阅
        return result;
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }
}

/**
 * 服务端：
 * 作为消费者，监听队列rpc_queue，接收来自客户端的计算请求（声明队列rpc_queue，最好先启动服务端，在启动客户端）
 * 作为生产者，计算处理完成后，将结果发布到队列（临时队列，从replyTo获取）中
 */
class RPCServer {

    private static final String QUEUE_NAME = "rpc_queue";

    // 递归计算斐波那契数列
    private static int fib(int n) {
        if (n == 0) {
            return 0;
        }
        if (n == 1) {
            return 1;
        }
        return fib(n - 1) + fib(n - 2);
    }

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            // 声明一个队列rpc_queue
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            // 清除指定队列（rpc_queue）中的所有数据
            channel.queuePurge(QUEUE_NAME);

            channel.basicQos(1); // 一次只接收一条消息

            System.out.println(" [*] Awaiting RPC requests...");

            Object monitor = new Object();
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                // 2. 客户端作为消费者从队列rpc_queue接收计算请
                // 构建响应客户端的发布消息的参数
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                        .Builder()
                        .correlationId(delivery.getProperties().getCorrelationId()) // correlationId：来自客户端的设置，现在重新返回给客户端
                        .build();

                String response = "";

                try {
                    String numMsg = new String(delivery.getBody(), "UTF-8");
                    int num = Integer.parseInt(numMsg);

                    System.out.printf("<<< [S] Start to calculate: fib(" + numMsg + ")... replyTo=%s, correlationId=%s\n",
                            delivery.getProperties().getReplyTo(), delivery.getProperties().getCorrelationId());
                    response += fib(num); // 实际计算处理
                    System.out.printf(">>> [S] Done!!! response=%s\n\n", response);
                } catch (RuntimeException e) {
                    System.out.println("[S] 异常: " + e.toString());
                } finally {
                    // >>>>>>3. 发布：服务端计算完 fib() 后，将结果 response + correlationId 发布到队列 replyTo（由客户端创建的随机队列） 中，等待客户端接收该计算结果
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
            channel.basicConsume(QUEUE_NAME, false, deliverCallback, (consumerTag -> {
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