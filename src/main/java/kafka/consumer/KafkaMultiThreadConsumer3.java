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
import kafka.message.MessageAndMetadata;
import kafka.tools.ConsumerOffsetChecker;

public class KafkaMultiThreadConsumer3 implements Runnable {

    private static ConsumerConnector consumer;
    private static ExecutorService executor;
    private KafkaStream<byte[], byte[]> stream;
    private int threadNumber;
    private String groupId;

    public KafkaMultiThreadConsumer3(KafkaStream<byte[], byte[]> stream, int threadNumber, String groupId) {
        this.stream = stream;
        this.threadNumber = threadNumber;
        this.groupId = groupId;
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> data = it.next();
            System.out.println("Thread-" + threadNumber + "(" + groupId + ")\tpartition:" + data.partition() + "\tkey:" + data.key() + "\tmessage:" + new String(data.message()));
            // try {
            // int rand = new Random().nextInt(1000);
            // Thread.sleep(rand);
            // } catch (InterruptedException e) {
            // e.printStackTrace();
            // }
        }
        System.out.println("Shutting down Thread: " + threadNumber);
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
        String topic = "test2";
        final int NUM_THREADS = 2;

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(topic, new Integer(NUM_THREADS));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        executor = Executors.newFixedThreadPool(NUM_THREADS);

        int threadNumber = 0;
        for (int i = 0; i < streams.size(); i++) {
            final KafkaStream<byte[], byte[]> stream = streams.get(i);
            executor.submit(new KafkaMultiThreadConsumer3(stream, threadNumber, groupId + i));
            threadNumber++;
        }
    }
}
