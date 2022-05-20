package com.xixi.lab.rabbitmq.spring.ox06_rpc;

import org.springframework.amqp.rabbit.annotation.RabbitListener;


public class Tut6Server {

    /**
     * 监听 队列rpc_queue，接收来自客户端的计算请求
     */
    @RabbitListener(queues = "rpc_queue")
    // @SendTo("tut.rpc.replies") used when the client doesn't set replyTo.
    public int fibonacci(int n) {
        System.out.println(" [x] Received request for " + n);
        int result = fib(n);
        System.out.println(" [.] Returned " + result);
        return result;
    }

    public int fib(int n) {
        return n == 0 ? 0 : n == 1 ? 1 : (fib(n - 1) + fib(n - 2));
    }

}
