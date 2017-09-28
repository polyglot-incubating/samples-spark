package org.chiwooplatform.samples.ex02;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import org.chiwooplatform.samples.support.SparkContextHolder;
import org.junit.Test;

import scala.Tuple2;

/**
 * RDD에는 어떤 데이타 형식이라던지 저장이 가능한데, 그중에서 Pair RDD라는 RDD가 있다.
 * 
 * 이 RDD는 Key-Value 형태로 데이타를 저장하기 때문에, 병렬 데이타 처리 부분에서 그룹핑과 같은 추가적인 기능을 사용할 수 있다. 예를 들어
 * reduceByKey 와 같이 특정 키를 중심으로 데이타 연산 (각 키 값 기반으로 합이나 평균을 구한다던가) key 기반으로 join 을 한다던가와 같은
 * 그룹핑 연산에 유용하게 사용할 수 있다.
 *
 * Pair 함수는 PairFunction<RDD, Key, Value> 으로 정의 하며, 입력값은 RDD 이고, 그 결과는 Tuple<Key,Value> 로
 * 반환 하도록 되어 있다.
 * 
 * <code>
RDD.mapToParis( d-> new Tuple2(key,value))

 
PairFunction<RDD값, Tuple_Key, Tuple_Value> pairFunc
 
</code>
 */
@SuppressWarnings("serial")
public class Ex201KeyValueFilter implements Serializable {

    /**
     * Scala 에선 단순 하게 <code>val pairs = lines.map(rdd => (rdd.split(" ")(0), rdd))</code>와
     * 같이 쉽게 구현 되지만, Java 에선 map 대신에 mapToPair 함수를 써야 하며 "pairFunc"함수와 같이 그 구조가 조금 복잡 하다.
     * <code>
     * PairFunction<String, String, String> pairFunc = new PairFunction<String, String, String>() {
     *   ...
     * }
     * 
     * rdd.mapToPair(pairFunc)
     * </code>
     * @throws Exception
     */
    PairFunction<String, String, String> pairFunc = new PairFunction<String, String, String>() {
        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public Tuple2<String, String> call(String rddValue) {
            // System.out.println("-----" + rddValue);
            final String[] cols = rddValue.split(" ");
            return new Tuple2(cols[0], rddValue);
        }
    };

    @Test
    public void ut1001_pairFunc() throws Exception {
        final String[] inputStrings = { "coffee", "i really like coffee", "coffee > magic" };
        JavaSparkContext spark = SparkContextHolder.getLocalContext("Ex202KeyValueFilter");
        JavaRDD<String> input = spark.parallelize(Arrays.asList(inputStrings));
        JavaPairRDD<String, String> rdd = input.mapToPair(pairFunc);
        final Map<String, String> result = rdd
                // .filter(longWordFilter)
                .collectAsMap();
        for (Entry<String, String> entry : result.entrySet()) {
            System.out.printf("Pair Key: '%s', Value: '%s'\n", entry.getKey(), entry.getValue());
        }
        spark.close();
    }

    Function<Tuple2<String, String>, Boolean> tupleFilter = new Function<Tuple2<String, String>, Boolean>() {
        @Override
        public Boolean call(Tuple2<String, String> val) {
            // val._1() is Key
            // val._2() is Value
            return (val._2().length() < 20);
        }
    };

    @Test
    public void ut1002_tupleFilter() throws Exception {
        final String[] inputStrings = { "coffee", "i really like coffee", "coffee > magic" };
        JavaSparkContext spark = SparkContextHolder.getLocalContext("Ex202KeyValueFilter");
        JavaRDD<String> input = spark.parallelize(Arrays.asList(inputStrings));
        JavaPairRDD<String, String> rdd = input.mapToPair(pairFunc);
        final Map<String, String> result = rdd.filter(tupleFilter).collectAsMap();
        for (Entry<String, String> entry : result.entrySet()) {
            System.out.printf("Pair Key: '%s', Value: '%s'\n", entry.getKey(), entry.getValue());
        }
        spark.close();
    }
}
