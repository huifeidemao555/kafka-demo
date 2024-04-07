package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

public class Consumer {
    private Collection<String> topic = new ArrayList<>();
    private KafkaConsumer<String, String> kafkaConsumer = null;
    public Consumer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");  // 指定 Broker
        properties.put("group.id", "experiment");              // 指定消费组群 ID
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // 将 key 的字节数组转成 Java 对象
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  // 将 value 的字节数组转成 Java 对象

        topic.add("test");
        kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(topic);
    }

    public static void main(String[] args) {
        Consumer consumer = new Consumer();
        ConsumerRecords<String, String> poll = consumer.kafkaConsumer.poll(Duration.ofSeconds(10));
        for (ConsumerRecord<String, String> consumerRecord: poll) {
            System.out.println("receive data: " + consumerRecord);
        }
    }
}
