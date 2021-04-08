package com.kafka.kafkaDocs.module.config;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class RebalanceListener implements ConsumerRebalanceListener {
  private KafkaConsumer kafkaConsumer;
  private Map<TopicPartition, OffsetAndMetadata> currentOffsets  = new HashMap<>();


    public RebalanceListener(KafkaConsumer con) {
        this.kafkaConsumer =con;
    }

    public void addOffsets(String topic,int partition, long offset){
           currentOffsets.put(new TopicPartition(topic,partition),new OffsetAndMetadata(offset,"commit"));
    }

    public Map<TopicPartition,OffsetAndMetadata> getCurrentOffsets(){
        return currentOffsets;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        System.out.println(" <======Partition has been revoked===>");
        for (TopicPartition topicPartitions:collection){
            System.out.print(topicPartitions.partition()+", ");
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        System.out.println("<===== Partition has been assigned=====>");
        for (TopicPartition topicPartitions:collection){
            System.out.print(topicPartitions.partition()+", ");
        }
    }
}
