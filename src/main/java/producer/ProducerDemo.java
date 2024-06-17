package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Kaifang Wu
 * @version 1.0.0
 * @Description TODO
 * @createTime 2024-06-17
 */
public class ProducerDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer(properties);

        ProducerRecord record = new ProducerRecord("quickstart-events", "hello 222");

        Future send = producer.send(record);
        Object o = send.get();
        System.out.printf("o=", o);
    }
}
