package kafka.consumer;

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
        topicMap.put(topic, new Integer(numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        executor = Executors.newFixedThreadPool(numThreads);

        int threadNumber = 0;
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executor.submit(new ConsumerTest(stream, threadNumber));
            threadNumber++;
        }
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
    }

    class ConsumerTest implements Runnable {
        private KafkaStream<byte[], byte[]> stream;
        private int threadNumber;

        public ConsumerTest(KafkaStream<byte[], byte[]> stream, int threadNumber) {
            this.stream = stream;
            this.threadNumber = threadNumber;
        }

        @Override
        public void run() {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while (it.hasNext()) {
                System.out.println("Thread " + threadNumber + ": " + new String(it.next().message()));
                // try {
                // int rand = new Random().nextInt(1000);
                // Thread.sleep(rand);
                // } catch (InterruptedException e) {
                // e.printStackTrace();
                // }
            }
            System.out.println("Shutting down Thread: " + threadNumber);

        }
    }
}
