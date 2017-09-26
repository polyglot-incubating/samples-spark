package org.chiwooplatform.samples.ex02;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import org.chiwooplatform.samples.SparkContextHolder;
import org.chiwooplatform.samples.SparkUtils;
import org.junit.Test;

import scala.Tuple2;

public class Ex203WordCountOptimized {

    private static final Pattern SPACE = Pattern.compile(" ");

    @Test
    public void testWordCount() throws Exception {
        SparkSession spark = SparkContextHolder.getLocalSession("Ex203WordCountOptimized");
        JavaRDD<String> lines = spark.read().textFile("src/main/resources/LICENSE").javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();
        SparkUtils.log(output);
        spark.stop();
        spark.close();
    }

}
