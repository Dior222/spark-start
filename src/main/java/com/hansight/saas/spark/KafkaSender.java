package com.hansight.saas.spark;

import com.google.common.base.Joiner;
import kafka.producer.KeyedMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

/**
 * KafkaSender
 *
 * @author liufenglin
 * @email fenglin_liu@hansight.com
 * @date 16/8/18
 */
public class KafkaSender {

    private static final String[] STRING_ARRAY = {"abc", "hello", "world", "hansight", "fucking"};

    private static Random random = new Random();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String line = null;
        Producer<String, String> producer = new KafkaProducer(props);
        for (int i = 0; i < 1000; i++) {
            line = randomPick3();

            producer.send(new ProducerRecord<String, String>("test", i + "", line));
            try {
                Thread.sleep(random.nextInt(200));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("send a message : " + line);
        }
        producer.close();

    }

    private static String randomPick3() {
        List<String> words = new ArrayList<>();
        Joiner joiner = Joiner.on(" ");
        int size = STRING_ARRAY.length;
        for (int i = 0; i < 3; i++) {
            words.add(STRING_ARRAY[random.nextInt(size)]);
        }

        return joiner.join(words);

    }

}
