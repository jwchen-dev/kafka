package kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.tools.ConsumerOffsetChecker;
import kafka.utils.ZkUtils;

public class KafkaMultiThreadConsumer {

    private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;
    private ExecutorService executor2;
    private String groupId;

    public KafkaMultiThreadConsumer(String zookeeper, String groupId, String topic) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
        this.topic = topic;
        this.groupId = groupId;
    }

    public void run(int numThreads) {
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(topic, new Integer(numThreads));
//        topicMap.put("helloworld", new Integer(numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
//        List<KafkaStream<byte[], byte[]>> helloworldStreams = consumerMap.get("helloworld");
        executor = Executors.newFixedThreadPool(numThreads);
//        executor2 = Executors.newFixedThreadPool(numThreads);
        
        int threadNumber = 0;
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executor.submit(new ConsumerTest(stream, threadNumber, groupId));
            threadNumber++;
        }
        
//        int threadNumber2 = 0;
//        for (final KafkaStream<byte[], byte[]> stream : helloworldStreams) {
//            executor2.submit(new ConsumerTest(stream, threadNumber2, "helloworld"));
//            threadNumber2++;
//        }
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
//        String topic = "helloworld";

//        ZkClient zkClient=new ZkClient("localhost:2181", 5000, 5000);
//        String dataStr=zkClient.readData("/brokers/topics");
//        System.out.println(dataStr);
//        System.exit(0);
        
        
        KafkaMultiThreadConsumer c = new KafkaMultiThreadConsumer(zookeeper, groupId, topic);
        c.run(1);
        KafkaMultiThreadConsumer cc = new KafkaMultiThreadConsumer(zookeeper, groupId, topic);
        cc.run(1);

//        KafkaMultiThreadConsumer c2 = new KafkaMultiThreadConsumer(zookeeper, groupId + 2, topic);
//        c2.run(3);
//        
//        KafkaMultiThreadConsumer c3 = new KafkaMultiThreadConsumer(zookeeper, groupId + 3, topic);
//        c3.run(3);
//        
//        KafkaMultiThreadConsumer c4 = new KafkaMultiThreadConsumer(zookeeper, groupId + 4, topic);
//        c4.run(3);
    }

    class ConsumerTest implements Runnable {
        private KafkaStream<byte[], byte[]> stream;
        private int threadNumber;
        private String groupId;

        public ConsumerTest(KafkaStream<byte[], byte[]> stream, int threadNumber, String groupId) {
            this.stream = stream;
            this.threadNumber = threadNumber;
            this.groupId = groupId;
        }

        @Override
        public void run() {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while (it.hasNext()) {
                MessageAndMetadata<byte[], byte[]> data = it.next();
                System.out.println(Thread.currentThread().getName()+"\tThread-" + threadNumber + "(" + groupId + ")\tpartition:" + data.partition() + "\tkey:" + data.key() + "\tmessage:" + new String(data.message()));
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
