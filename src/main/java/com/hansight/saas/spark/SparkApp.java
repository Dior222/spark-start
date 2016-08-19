package com.hansight.saas.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * SparkApp
 *
 * @author liufenglin
 * @email fenglin_liu@hansight.com
 * @date 16/7/26
 */
public class SparkApp {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("spark-demo")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1,2,3,4,5,11,12,89);
        JavaRDD<Integer> distData = sc.parallelize(data);
        System.out.println(distData.toString());
        int sum = distData.reduce((a, b) -> a + b);
        System.out.println("sum is : " + sum);

        System.out.println("==========================");

        JavaRDD<String> diskData = sc.textFile("data/data.txt");
        System.out.println("content in file is : ");
        diskData.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println("\t" + s);
            }
        });
        JavaRDD<Integer> lengthData = diskData.map(new Function<String, Integer>() {
            @Override
            public Integer call(String v1) throws Exception {
                return v1.length();
            }
        });

        int length = lengthData.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println("length : " + length);

        System.out.println("================== PI Estimate ===================");

        long startTime = System.currentTimeMillis();
        PiEstimation piEstimation = new PiEstimation(sc);
        System.out.println(piEstimation.estimate(1000000));
        System.out.println("time spent : " + (System.currentTimeMillis() - startTime));

    }
}
