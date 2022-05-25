package com.xixi.lab.rabbitmq.spring.ox06_rpc;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.util.StopWatch;

/**
 * 服务端
 */
public class Tut6Server {

    /**
     * 监听 队列 spring-rpc-request-queue，接收来自客户端的计算请求
     */
    @RabbitListener(queues = Tut6Config.REQUEST_QUEUE)
    // @SendTo("tut.rpc.replies") used when the client doesn't set replyTo.
    public int fibonacci(int n) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        System.out.println("<<< [S] Received request for " + n);
        int result = fib(n);
        stopWatch.stop();
        System.out.printf(" [S] Returned: result=%d, cost=%fs\n\n", result, stopWatch.getTotalTimeSeconds());
        return result;
    }

    public int fib(int n) {
        return n == 0 ? 0 : n == 1 ? 1 : (fib(n - 1) + fib(n - 2));
    }
}
