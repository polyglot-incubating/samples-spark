package org.chiwooplatform.samples.ex02;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import org.chiwooplatform.samples.SparkContextHolder;
import org.chiwooplatform.samples.SparkUtils;
import org.junit.Test;

import scala.Tuple2;

/**
 * 
 */
public class Ex202WordCountMapReduce {

    /**
     * RDD 의 값을 split 하여 분할한 결과 타입<T> 를 Iterator 할 수 있는 인터페이스(엑션)을 반환 한다.
     */
    @SuppressWarnings("serial")
    private static final FlatMapFunction<String, String> splitWords = new FlatMapFunction<String, String>() {
        public Iterator<String> call(String value) {
            return Arrays.asList(value.split(" ")).iterator();
        }
    };

    /**
     * Group(Key)에 대한 수학적 통계를 계산 하기 위한 DataFrame 을 구성 하기 위한 팁. (MapReduce 에서 자주 활용 되는 팁
     * 이기도 함.)
     * @throws Exception
     */
    @SuppressWarnings("serial")
    private static final PairFunction<String, String, Integer> groupTuple = new PairFunction<String, String, Integer>() {
        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public Tuple2<String, Integer> call(String val) {
            // System.out.println("-----" + val);
            return new Tuple2(val, 1);
        }
    };

    /**
     * Function<A, B, R> f; 함수 R = f(a,b)
     */
    @SuppressWarnings("serial")
    private static final Function2<Integer, Integer, Integer> reduceSum = new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer x, Integer y) {
            return x + y;
        }
    };

    /**
     * reduceByKey 는 식별된 key를 기준으로 reduce 연산을 할 수 있도록 해 준다.
     * @throws Exception
     */
    @Test
    public void testWordCount() throws Exception {
        JavaSparkContext spark = SparkContextHolder.getLocalContext("Ex202KeyValueFilter");
        JavaRDD<String> rdd = spark.textFile("src/main/resources/LICENSE");
        JavaPairRDD<String, Integer> counts = rdd.flatMap(splitWords).mapToPair(groupTuple).reduceByKey(reduceSum);
        // counts.saveAsTextFile("src/test/resources/LICENSE.WordCount");
        final List<Tuple2<String, Integer>> result = counts.collect();
        SparkUtils.log(result);
        spark.close();
    }

    @SuppressWarnings("serial")
    private static final FlatMapFunction<Row, Row> rowSplitWords = new FlatMapFunction<Row, Row>() {
        public Iterator<Row> call(Row value) {
            Object[] cols = value.getString(0).trim().split(" ");
            return Arrays.asList(RowFactory.create(cols)).iterator();
        }
    };

    @Test
    public void testWordCountBySql() throws Exception {
        SparkSession spark = SparkContextHolder.getLocalSession("Ex202KeyValueFilter");
        JavaRDD<Row> javaRDD = spark.read().text("src/main/resources/LICENSE").toJavaRDD().flatMap(rowSplitWords);
        // CHECK data
        // javaRDD.take(20).forEach(v -> System.out.println(v));

        final StructType schema = SparkUtils.stringSchema("word");
        Dataset<Row> rdd = spark.createDataFrame(javaRDD, schema);
        rdd.createOrReplaceTempView("word");
        rdd.show();
        spark.close();
    }

}
