package com.hansight.saas.spark;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * SparkStreamingKafkaProcessor
 *
 * @author liufenglin
 * @email fenglin_liu@hansight.com
 * @date 16/8/18
 */
public class SparkStreamingKafkaProcessor implements Serializable {

    private transient JavaStreamingContext jssc;

    private static final Pattern SPACE = Pattern.compile(" ");

    private final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

    public SparkStreamingKafkaProcessor() {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        jssc = new JavaStreamingContext(conf, Durations.seconds(5));
    }

    public void process1() {
        String topics = "test";
        String brokers = "127.0.0.1:9092";

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
//            @Override
//            public java.util.Iterator<String> call(String x) {
//                return Arrays.asList(SPACE.split(x)).iterator();
//            }
        });
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                }).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        wordCounts.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();

    }

    public void process() {

        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put("test", 2);

        JavaPairReceiverInputDStream<String, String> kafkaStream = KafkaUtils.createStream(
                jssc, "127.0.0.1:2181", "saas", topicMap
        );

        JavaDStream<String> lines = kafkaStream.map(new Function<Tuple2<String,String>, String>() {
            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                return v1._2();
            }
        });

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split("\\s+"));
            }
        });

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordCounts.print();

//        kafkaStream.transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
//            @Override
//            public JavaPairRDD<String, String> call(JavaPairRDD<String, String> v1) throws Exception {
//                OffsetRange[] offsets = ((HasOffsetRanges) v1.rdd()).offsetRanges();
//                offsetRanges.set(offsets);
//                return v1;
//            }
//        }).foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
//            @Override
//            public Void call(JavaPairRDD<String, String> v1) throws Exception {
//                for (OffsetRange o : offsetRanges.get()) {
//                    System.out.println(
//                            o.topic() + "-" + o.partition() + ": from " + o.fromOffset() + " to " + o.untilOffset()
//                    );
//                }
//                return null;
//            }
//        });

        jssc.start();
        jssc.awaitTermination();

    }

    public static void main(String[] args) {
        SparkStreamingKafkaProcessor processor = new SparkStreamingKafkaProcessor();
        processor.process();
    }

}
