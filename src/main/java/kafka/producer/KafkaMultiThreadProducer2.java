package kafka.producer;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaMultiThreadProducer2 implements Runnable {

    private static Producer<String, String> producer = null;

    private String topic = null;
    private int messageCount = -1;
    private Integer id = 0;

    public KafkaMultiThreadProducer2(String topic, int messageCount, int id) {
        Properties props = new Properties();
        // Set the broker list for requesting metadata to find the lead broker
        props.put("metadata.broker.list", "localhost:9092,localhost:9091,localhost:9090,localhost:9089");
        // This specifies the serializer class for keys
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // 1 means the producer receives an acknowledgment once the lead replica
        // has received the data. This option provides better durability as the
        // client waits until the server acknowledges the request as successful.
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);

        this.topic = topic;
        this.messageCount = messageCount;
        this.id = id;
    }

    @Override
    public void run() {
        for (int mCount = 0; mCount < messageCount; mCount++) {
            String runtime = Long.toString(System.nanoTime());
            String msg = "Message Publishing Time - " + runtime;
            System.out.println(msg);
            // Creates a KeyedMessage instance
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, Integer.toString(id), msg);
            // Publish the message
            producer.send(data);
        }
        // Close producer connection with broker.
        producer.close();
    }

    public static void main(String[] args) {
        final long START=System.currentTimeMillis();
//        final String TOPIC = "test2";
        final String TOPIC = "helloworld";
        final int COUNT = 100;
        final int POOL_SIZE = 10;

        System.out.println("Topic Name - " + TOPIC);
        System.out.println("Message Count - " + COUNT);

        // create thread pool
        ExecutorService executor = Executors.newFixedThreadPool(POOL_SIZE);

        // submit job
        for (int i = 0; i < POOL_SIZE; i++) {
            KafkaMultiThreadProducer2 producer = new KafkaMultiThreadProducer2(TOPIC, COUNT, i);
            executor.submit(producer);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        try {
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println(System.currentTimeMillis()-START);
    }

    class SimplePartitioner implements Partitioner {

        @Override
        public int partition(Object arg0, int arg1) {

            return 0;
        }

    }
}
