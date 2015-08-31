package com.uitox.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class KafkaMultiThreadConsumer {

    private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;

    public KafkaMultiThreadConsumer(String zookeeper, String groupId, String topic) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
        this.topic = topic;
    }

    public void run(int numThreads) {
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        executor = Executors.newFixedThreadPool(numThreads);

        for (final KafkaStream<byte[], byte[]> stream : streams) {
            ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
            int threadNumber = 0;
            while (consumerIte.hasNext()) {
                executor.submit(new ConsumerTest(threadNumber, consumerIte.next()));
                threadNumber++;
            }
        }

        if (consumer != null)
            consumer.shutdown();
    }

    private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        shutdown();
    }

    public void shutdown() {
        if (consumer != null) {
            consumer.shutdown();
        }

        if (executor != null) {
            executor.shutdown();
        }

        try {
            if (executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String zookeeper = "localhost:2181";
        String groupId = "testgroup";
        String topic = "test";

        KafkaMultiThreadConsumer c = new KafkaMultiThreadConsumer(zookeeper, groupId, topic);
        c.run(3);

        // try {
        // Thread.sleep(10000);
        // } catch (InterruptedException e) {
        // e.printStackTrace();
        // }
        //
        // c.shutdown();
    }

}

class ConsumerTest implements Runnable {
    private MessageAndMetadata<byte[], byte[]> message;
    private int threadNumber;

    public ConsumerTest(int threadNumber, MessageAndMetadata<byte[], byte[]> message) {
        this.message = message;
        this.threadNumber = threadNumber;
    }

    @Override
    public void run() {
        int rand = new Random().nextInt(1000);
        System.out.println("Thread-" + Thread.currentThread().getId() + ": " + new String(message.message()));
        try {
            Thread.sleep(rand);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
