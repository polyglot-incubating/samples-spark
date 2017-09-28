package org.chiwooplatform.samples.ex01;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.chiwooplatform.samples.support.SparkContextHolder;
import org.chiwooplatform.samples.support.SparkUtils;
import org.junit.Test;

public class Ex004RddUnion {

    /**
     * 파일을 RDD 필터링 예제
     */
    @Test
    public void testRDD() throws Exception {
        JavaSparkContext spark = SparkContextHolder.getLocalContext("Ex004RddUnion");
        JavaRDD<String> rdd1 = spark.textFile("src/main/resources/log.txt")
                .filter((Function<String, Boolean>) v -> v.contains("systemd")).cache();
        JavaRDD<String> rdd2 = spark.textFile("src/main/resources/log.txt")
                .filter((Function<String, Boolean>) v -> v.contains("dhclient")).cache();
        JavaRDD<String> rdd = rdd1.union(rdd2);
        SparkUtils.log(rdd.collect());
        spark.close();
    }

}
