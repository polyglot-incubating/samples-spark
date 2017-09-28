package org.chiwooplatform.samples.ex01;

import java.util.Arrays;
import java.util.Iterator;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import org.chiwooplatform.samples.support.SparkContextHolder;
import org.chiwooplatform.samples.support.SparkUtils;
import org.junit.Test;

/**
 * flatMap 은 각 입력 요소에 대해 여러개의 출력 요소를 만들 때 주로 사용 한다.
 */
@SuppressWarnings("serial")
public class Ex007FlatMap implements Serializable {

    /**
     * input -> map -> filter -> result 테스트
     * @throws Exception
     */
    @Test
    public void testRDD() throws Exception {
        JavaSparkContext spark = SparkContextHolder.getLocalContext("Ex007FlatMap");
        JavaRDD<String> rdd = spark.parallelize(Arrays.asList("Hello world", "hi"));

        FlatMapFunction<String, String> func = new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        };

        JavaRDD<String> words = rdd.flatMap(func);
        SparkUtils.log(words.collect());
        spark.close();
    }

}
