package org.chiwooplatform.samples.ex01;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.chiwooplatform.samples.SparkContextHolder;
import org.chiwooplatform.samples.SparkUtils;
import org.junit.Test;

public class Ex006MapAndFilter {

    /**
     * input -> map -> filter -> result 테스트
     * @throws Exception
     */
    @Test
    public void testRDD() throws Exception {
        JavaSparkContext spark = SparkContextHolder.getLocalContext("Ex006MapAndFilter");
        JavaRDD<Integer> rdd = spark.parallelize(Arrays.asList(1, 2, 3, 4)).map((Function<Integer, Integer>) f -> f * f)
                .filter((Function<Integer, Boolean>) v -> v != 1);
        SparkUtils.log(rdd.collect());
        spark.close();
    }

}
