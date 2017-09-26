package org.chiwooplatform.samples.ex01;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.chiwooplatform.samples.SparkContextHolder;
import org.chiwooplatform.samples.SparkUtils;
import org.junit.Test;

public class Ex003RddBasicFilter {

    /**
     * 파일을 RDD 필터링 예제
     */
    @Test
    public void testRDD() throws Exception {
        JavaSparkContext spark = SparkContextHolder.getLocalContext("Ex003RddBasicFilter");
        JavaRDD<String> rdd = spark.textFile("src/main/resources/log.txt")
                .filter((Function<String, Boolean>) v -> v.contains("systemd")).cache();
        SparkUtils.log(rdd.collect());
        spark.close();
    }

}
