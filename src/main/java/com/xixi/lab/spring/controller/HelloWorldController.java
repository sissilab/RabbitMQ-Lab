package com.xixi.lab.spring.controller;

import com.xixi.lab.spring.patterns.hello_world.HelloWorldSender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequestMapping("helloWorld")
@RestController
public class HelloWorldController {

    @Autowired
    private HelloWorldSender helloWorldSender;


    @GetMapping("/send/{msg}")
    public void send(@PathVariable("msg") String msg) {
        helloWorldSender.send(msg);
    }

}
