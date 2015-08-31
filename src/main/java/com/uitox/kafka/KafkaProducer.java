package com.uitox.kafka;

import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {

    private static Producer<String, String> producer;

    public KafkaProducer() {
        Properties props = new Properties();
        // Set the broker list for requesting metadata to find the lead broker
        props.put("metadata.broker.list", "localhost:9092");
        // This specifies the serializer class for keys
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // 1 means the producer receives an acknowledgment once the lead replica
        // has received the data. This option provides better durability as the
        // client waits until the server acknowledges the request as successful.
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
    }

    private void publishMessage(String topic, int messageCount) {
        for (int mCount = 0; mCount < messageCount; mCount++) {
            String runtime = new Date().toString();
            String msg = "Message Publishing Time - " + runtime;
            System.out.println(msg);
            // Creates a KeyedMessage instance
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg);
            // Publish the message
            producer.send(data);
        }
        // Close producer connection with broker.
        producer.close();
    }

    public static void main(String[] args) {
        String topic = "test";
        String count = "10";
        int messageCount = Integer.parseInt(count);
        System.out.println("Topic Name - " + topic);
        System.out.println("Message Count - " + messageCount);
        KafkaProducer simpleProducer = new KafkaProducer();
        simpleProducer.publishMessage(topic, messageCount);
    }
}
