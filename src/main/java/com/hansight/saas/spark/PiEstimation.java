package com.hansight.saas.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.beans.Transient;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * PiEstimation
 *
 * @author liufenglin
 * @email fenglin_liu@hansight.com
 * @date 16/8/18
 */
public class PiEstimation implements Serializable {

    private transient JavaSparkContext sc;

    public PiEstimation(JavaSparkContext sc) {
        this.sc = sc;
    }

    public double estimate(int num) {
        List<Integer> list = new ArrayList<>();
        for(int i = 0 ; i < num ; i++) {
            list.add(i);
        }

        long count = sc.parallelize(list).filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                double x = Math.random();
                double y = Math.random();
                return x * x + y * y < 1;
            }
        }).count();

        return 4.0 * count / num ;
    }
}
