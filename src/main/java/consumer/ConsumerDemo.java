package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Kaifang Wu
 * @version 1.0.0
 * @Description TODO
 * @createTime 2024-06-17
 */
public class ConsumerDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "one");

        KafkaConsumer consumer = new KafkaConsumer(properties);

        consumer.subscribe(Arrays.asList("quickstart-events"));

        while (true) {
            ConsumerRecords records = consumer.poll(Duration.ofSeconds(1));

            if (!records.isEmpty()) {
                Iterator iterator = records.iterator();
                while (iterator.hasNext()) {
                    ConsumerRecord msg = (ConsumerRecord) iterator.next();
                    System.out.println("message=" + msg.value());
                }
                consumer.commitSync();
            }
            
        }
    }
}
