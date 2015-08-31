package kafka.producer;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaMultiThreadProducer implements Runnable {

    private static Producer<String, String> producer = null;

    private String topic = null;
    private int messageCount = -1;

    public KafkaMultiThreadProducer(String topic, int messageCount) {
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

        this.topic = topic;
        this.messageCount = messageCount;
    }

    @Override
    public void run() {
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
        final String TOPIC = "test";
        final int COUNT = 100;
        final int POOL_SIZE = 3;

        System.out.println("Topic Name - " + TOPIC);
        System.out.println("Message Count - " + COUNT);

        // create thread pool
        ExecutorService executor = Executors.newFixedThreadPool(POOL_SIZE);

        // submit job
        for (int i = 0; i < POOL_SIZE; i++) {
            KafkaMultiThreadProducer producer = new KafkaMultiThreadProducer(TOPIC, COUNT);
            executor.submit(producer);
        }

        try {
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
