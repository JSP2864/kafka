package com.kafka.kafkaDocs.module.controller;

import com.kafka.kafkaDocs.module.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.xml.ws.Response;
import java.lang.reflect.Method;

@RestController
@RequestMapping(value="/app/rest")
public class ProducerController {

     @Autowired
     private ProducerService producerService;

    @RequestMapping(value="/testing",method = RequestMethod.GET)
    public ResponseEntity testing(){
        System.out.println("working");
        producerService.initiateKafka();
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @RequestMapping(value="/producer",method = RequestMethod.GET)
    public ResponseEntity produceMessage(@RequestParam String message,@RequestParam String topic,@RequestParam String key){
        System.out.println("working");
        producerService.createProducer(message,topic,key);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @RequestMapping(value="/consumer",method = RequestMethod.GET)
    public ResponseEntity consumerMessage(){
        System.out.println("consumer");
        producerService.createConsumer();
        return new ResponseEntity(HttpStatus.OK);
    }
}
