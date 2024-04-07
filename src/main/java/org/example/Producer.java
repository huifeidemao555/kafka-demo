package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {
    private String topic = "";
    private KafkaProducer<String, String> kafkaProducer = null;

    private Producer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");    // 指定 Broker
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");  // 将 key 的 Java 对象转成字节数组
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // 将 value 的 Java 对象转成字节数组
        properties.put("acks", "1");       // 消息至少成功发给一个副本后才返回成功
        properties.put("retries", "5");    // 消息重试 5 次
        topic = "test";
        kafkaProducer = new KafkaProducer<>(properties);
    }

    public static void main(String[] args) {
        Producer producer1 = new Producer();
        for(int i = 1; i <= 100; i++) {
            producer1.kafkaProducer.send(new ProducerRecord<>(producer1.topic, Integer.toString(i)));
            System.out.println("send data: " + i);
        }
        producer1.kafkaProducer.close();
    }
}
