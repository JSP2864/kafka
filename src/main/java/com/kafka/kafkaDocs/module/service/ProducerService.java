package com.kafka.kafkaDocs.module.service;

import com.kafka.kafkaDocs.module.config.RebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerProperties;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Properties;

@Service
public class ProducerService {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private Properties properties;

    @Autowired
    @Qualifier("ConsumerPropertiesBean")
    private Properties consumerProperties;

    private final static String TOPIC="kafka_topic*";

    public  void  initiateKafka(){
       kafkaTemplate.send(TOPIC,"testing1");
    }

    /**
     * Require 3 Parameters -> Properties ,Producer, and ProducerRecord
     * Properties include detail of server,serializer detail
     * Producer is HashMap(in this case <String,String>) of new KafkaProducer and pass properties object
     * ProducerRecord is also HasMap of new ProducerRecord and pass Topic,key,value
     * ===========================
     * record is data, producer is medium to send data, properties knows where to send data.
     */
    public void createProducer(String message,String topic,String key) {
//        String key = "key";
//        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "localhost:9092");
//        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic,2,key,message);
        producer.send(record);
        producer.close();
        System.out.println("message send");
    }

    public void createConsumer(){
//        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "localhost:9092");
//        properties.put("group.id","jsp");
//        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(consumerProperties);
        RebalanceListener rebalanceListener = new RebalanceListener(consumer);
        consumer.subscribe(Arrays.asList("music","kafka_topic1","test"),rebalanceListener);
        try {
            while (true){
                ConsumerRecords<String,String> consumerRecords = consumer.poll(10000);
//                Thread.sleep(2000);
                for(ConsumerRecord<String,String> records:consumerRecords){
                    System.out.println("record fetched: "+records.value()+" partition:"+records.partition()+" offset :"
                            +records.offset()+" topic :"+records.topic());
                    rebalanceListener.addOffsets(records.topic(),records.partition(),records.offset());
                }
                consumer.commitSync(rebalanceListener.getCurrentOffsets());
            }
        } catch (Exception e){
            System.out.println("Exception =>"+e.getMessage());
        }
        finally {
            consumer.close();
        }
    }
}
