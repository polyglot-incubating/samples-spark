package org.chiwooplatform.samples.ex01;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.chiwooplatform.samples.support.SparkContextHolder;
import org.chiwooplatform.samples.support.SparkUtils;
import org.junit.Test;

public class Ex002RddBasic {

    /**
     * 컬렉션을 RDD 로 변환 하는 예제
     */
    @Test
    public void testRDDFromCollection() throws Exception {
        JavaSparkContext spark = SparkContextHolder.getLocalContext("Ex001RddBasic");
        JavaRDD<Integer> rdd = spark.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> result = rdd.map((Function<Integer, Integer>) f -> f * f);
        System.out.println(StringUtils.join(result.collect(), ","));
        spark.close();
    }

    /**
     * 파일을 RDD 로 변환 하는 예제
     */
    @Test
    public void testRDDFromFile() throws Exception {
        JavaSparkContext spark = SparkContextHolder.getLocalContext("Ex002RddBasic");
        JavaRDD<String> rdd = spark.textFile("src/main/resources/LICENSE");
        // System.out.println(rdd.toDebugString());
        SparkUtils.log(rdd.take(5));
        spark.close();
    }

}
